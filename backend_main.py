import os
import json
import asyncio
import random
import feedparser
import re
import sqlite3
import requests
import hashlib
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import google.generativeai as genai

# --- OFFICIAL IDFM TRANSIT API KEY ---
IDFM_API_KEY = "oHymtFY3DTr0tdSr5jghmDe1Qaxhxn9X"

# --- PARIS TRANSIT GEOLOCATION MATRIX ---
PARIS_TRANSIT_NODES = {
    "1": {"lat": 48.8654, "lng": 2.3211},  "2": {"lat": 48.8820, "lng": 2.3323},
    "3": {"lat": 48.8649, "lng": 2.3985},  "4": {"lat": 48.8530, "lng": 2.3444},
    "5": {"lat": 48.8322, "lng": 2.3556},  "6": {"lat": 48.8410, "lng": 2.3190},
    "7": {"lat": 48.8590, "lng": 2.3580},  "8": {"lat": 48.8675, "lng": 2.3136},
    "9": {"lat": 48.8710, "lng": 2.3300},  "10": {"lat": 48.8510, "lng": 2.2980},
    "11": {"lat": 48.8670, "lng": 2.3650}, "12": {"lat": 48.8430, "lng": 2.3410},
    "13": {"lat": 48.8800, "lng": 2.3150}, "14": {"lat": 48.8335, "lng": 2.3734},
    "A": {"lat": 48.8738, "lng": 2.2950},  "B": {"lat": 48.8188, "lng": 2.3387},
    "C": {"lat": 48.8533, "lng": 2.2764},  "D": {"lat": 48.8809, "lng": 2.3553},
    "E": {"lat": 48.8768, "lng": 2.3592},
}

INCIDENT_CACHE = []
MAX_CACHE_SIZE = 50

# --- AI SETUP ---
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))

SYSTEM_INSTRUCTION = """
You are an elite OSINT geospatial intelligence parser for the Grey Lane platform in Paris. 
Analyze the provided raw text (social media, news, or Telegram chatter) and extract the incident details into STRICT JSON format.

Crucial Protest Routing Rules:
If the text describes a moving event (e.g., a protest march, a parade, a moving riot), you MUST identify the starting location and the destination location, and output them as start_lat, start_lng, and end_lat, end_lng.
If it is a stationary event (like a street robbery or a static gathering), set the end_lat and end_lng to the exact same values as the start_lat and start_lng.

You must return ONLY a JSON object with exactly these keys:
{
    "category": "Choose one: CIVIL_UNREST, PROPERTY_DAMAGE, CRIME, HIGH_SECURITY",
    "incident_type": "Short 2-3 word description (e.g., 'Moving Protest', 'Violent Riot', 'Pickpocket')",
    "severity": "Rate 1 to 5 (5 being highly critical)",
    "start_lat": "Latitude of the origin point (float)",
    "start_lng": "Longitude of the origin point (float)",
    "end_lat": "Latitude of the destination point (float)",
    "end_lng": "Longitude of the destination point (float)",
    "description": "A brief, tactical summary of the event."
}
"""

ai_model = genai.GenerativeModel('gemini-3.1-flash-lite', system_instruction=SYSTEM_INSTRUCTION)

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        for cached_incident in INCIDENT_CACHE:
            await websocket.send_json({"event": "new_incident", "incident": cached_incident})
        for incident in get_stored_incidents():
            await websocket.send_json(incident)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

manager = ConnectionManager()
PARIS_CENTER_LAT, PARIS_CENTER_LNG = 48.8566, 2.3522
DB_FILE = "greylane_local.db"

async def broadcast_new_incident(incident_data):
    INCIDENT_CACHE.append(incident_data)
    if len(INCIDENT_CACHE) > MAX_CACHE_SIZE:
        INCIDENT_CACHE.pop(0)
    await manager.broadcast({"event": "new_incident", "incident": incident_data})

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS incidents (
            id INTEGER PRIMARY KEY AUTOINCREMENT, lat REAL, lng REAL,
            category TEXT, description TEXT, severity TEXT, ingested_at TEXT
        )
    """)
    conn.commit()
    conn.close()

def store_incident(inc):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO incidents (lat, lng, category, description, severity, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (inc['lat'], inc['lng'], inc['category'], inc['description'], inc['severity'], datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()

def get_stored_incidents():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT lat, lng, category, description, severity FROM incidents")
    rows = cursor.fetchall()
    conn.close()
    return [{"event": "new_incident", "incident": {"lat": r[0], "lng": r[1], "category": r[2], "description": r[3], "severity": r[4]}} for r in rows]

def database_auto_scrub():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    time_threshold = (datetime.utcnow() - timedelta(hours=24)).isoformat()
    cursor.execute("DELETE FROM incidents WHERE ingested_at < ?", (time_threshold,))
    deleted_count = cursor.rowcount
    conn.commit()
    conn.close()
    if deleted_count > 0:
        print(f"🧹 DATABASE AUTO-SCRUB: Cleared {deleted_count} expired threat profiles (>24h).")

NEWS_FEEDS = ["https://www.france24.com/en/rss", "https://www.rfi.fr/en/france/rss"]
TRANSIT_FEED = "https://www.asf-en-direct.fr/rss/trafic-ratp.xml" 
MICRO_FEEDS = ["https://www.leparisien.fr/paris-75/rss.xml", "https://www.lefigaro.fr/paris/rss.xml"]

async def process_raw_report(title, description, source_type, source_name):
    clean_text = re.sub(r'<[^>]+>', ' ', title + " " + description).strip()
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM incidents WHERE description LIKE ?", (f"%{clean_text[:20]}%",))
    exists = cursor.fetchone()[0]
    conn.close()
    if exists or len(clean_text) < 10: return

    security_keywords = [
        "protest", "manifestation", "strike", "grève", "riot", "émeute", "march", "défilé",
        "police", "gendarmerie", "pompiers", "attack", "agression", "arrest", "interpellé",
        "stolen", "vol", "cambriolage", "robbery", "braquage", "crime", "délit",
        "accident", "crash", "collision", "choc", "blessé", "injured", "incident", "emergency",
        "damage", "dégât", "destruction", "vandal", "dégradation", "fire", "incendie", "explosion",
        "transit", "trafic", "métro", "metro", "rer", "bus", "tram", "traffic", "congestion",
        "fermé", "closed", "suspendu", "suspended", "delayed", "retard", "panne", "breakdown",
        "paris", "île-de-france", "security", "sécurité", "suspect", "risk", "danger"
    ]
    if not any(k in (title + " " + description).lower() for k in security_keywords): return

    print(f"🧠 AI Analyzing {source_type} [{source_name}]: {title[:50]}...")
    prompt = f"Analyze this urban incident report from Paris:\nTitle: {title}\nDescription: {description}\nRespond ONLY with a JSON map containing relevant_to_paris, exact_location_found, lat, lng, severity, category."
    
    try:
        response = await asyncio.to_thread(ai_model.generate_content, prompt)
        raw_json = response.text.strip()
        if "{" in raw_json: raw_json = raw_json[raw_json.find("{"):raw_json.rfind("}")+1]
        ai_data = json.loads(raw_json)
        
        if not ai_data.get("relevant_to_paris", False): return
        lat = float(ai_data.get("lat", PARIS_CENTER_LAT))
        lng = float(ai_data.get("lng", PARIS_CENTER_LNG))
        if not ai_data.get("exact_location_found", False):
            lat += random.uniform(-0.005, 0.005)
            lng += random.uniform(-0.005, 0.005)

        incident_data = {
            "lat": lat, "lng": lng, "category": ai_data.get("category", source_type).upper(),
            "description": f"<b>🚨 {source_type} ALERT: {title}</b><br><br>{description[:160]}...<br><br>Source: {source_name}",
            "severity": ai_data.get("severity", "medium").lower()
        }
        store_incident(incident_data)
        await broadcast_new_incident(incident_data)
    except Exception as e:
        print(f"⚠️ Intel processing skipped: {e}")

def fetch_idfm_transit_status():
    url = "https://prim.iledefrance-mobilites.fr/marketplace/v2/navitia/disruptions"
    headers = {"apiKey": IDFM_API_KEY}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            disruptions = response.json().get("disruptions", [])
            for dist in disruptions:
                messages = dist.get("messages", [])
                if not messages: continue
                text_alert = messages[0].get("text", "Transit Disruption")
                line_name = ""
                impacted = dist.get("impacted_objects", [])
                if impacted and "line" in impacted[0].get("pt_object", {}):
                    line_name = impacted[0]["pt_object"]["line"].get("name", "")
                
                line_code = str(line_name).upper().replace("LINE ", "").strip()
                if line_code in PARIS_TRANSIT_NODES:
                    lat, lon = PARIS_TRANSIT_NODES[line_code]["lat"], PARIS_TRANSIT_NODES[line_code]["lng"]
                else:
                    hash_val = int(hashlib.md5(line_code.encode()).hexdigest(), 16)
                    lat = 48.8566 + ((hash_val % 100) - 50) * 0.001
                    lon = 2.3522 + (((hash_val // 100) % 100) - 50) * 0.001
                
                incident_data = {"lat": lat, "lng": lon, "category": "TRANSIT", "description": f"[IDFM] {text_alert}<br><strong>Line:</strong> {line_name}", "severity": dist.get("severity", {}).get("name", "warning")}
                store_incident(incident_data)
    except Exception as e:
        print(f"⚠️ IDFM Connection Error: {e}")

async def scrape_transit_data():
    fetch_idfm_transit_status()
    try:
        transit_feed = feedparser.parse(TRANSIT_FEED)
        if transit_feed.entries:
            for entry in reversed(transit_feed.entries[:10]):
                await process_raw_report(getattr(entry, 'title', ''), getattr(entry, 'description', ''), "TRANSIT", "RATP Live Traffic")
    except Exception as e: print(f"⚠️ Transit feed error: {e}")

async def scrape_intelligence_feeds():
    for feed_url in NEWS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            if feed.entries:
                for entry in reversed(feed.entries[:5]):
                    await process_raw_report(getattr(entry, 'title', ''), getattr(entry, 'description', ''), "NEWS", "France24" if "france24" in feed_url else "RFI")
                    await asyncio.sleep(2)
        except Exception as e: print(f"⚠️ Macro feed error: {e}")
    for feed_url in MICRO_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            if feed.entries:
                for entry in reversed(feed.entries[:5]):
                    await process_raw_report(getattr(entry, 'title', ''), getattr(entry, 'description', ''), "LOCAL INTEL", "LeParisien" if "leparisien" in feed_url else "LeFigaro")
                    await asyncio.sleep(2)
        except Exception as e: print(f"⚠️ Micro feed error: {e}")

async def intelligence_gathering_loop():
    while True:
        try:
            database_auto_scrub()
            await scrape_transit_data()
            await scrape_intelligence_feeds()
            await asyncio.sleep(300)
        except Exception as e:
            await asyncio.sleep(60)

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    asyncio.create_task(intelligence_gathering_loop())
    yield

app = FastAPI(title="Grey Lane Backend", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.get("/")
def read_root(): return {"status": "Online"}

# --- MANUAL AI PARSER ENDPOINT ---
class ParseRequest(BaseModel):
    text: str

@app.post("/api/parse-incident")
async def manual_parse(req: ParseRequest):
    print(f"🧠 Manual AI Parse Request Received: {req.text[:50]}...")
    prompt = f"Analyze this urban incident report from Paris:\n\"{req.text}\"\nRespond ONLY with a valid JSON object matching the standard layout."
    try:
        response = await asyncio.to_thread(ai_model.generate_content, prompt)
        raw_json = response.text.strip()
        if "{" in raw_json: raw_json = raw_json[raw_json.find("{"):raw_json.rfind("}")+1]
        return json.loads(raw_json)
    except Exception as e:
        return {"error": str(e)}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
