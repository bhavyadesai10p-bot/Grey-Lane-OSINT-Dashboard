import os
import json
import asyncio
import random
import feedparser
import re
import sqlite3
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import google.generativeai as genai

# --- AI SETUP ---
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
ai_model = genai.GenerativeModel('gemini-3.1-flash-lite')

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        # On connection, stream the unexpired history from database
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

# --- SYSTEM 1: AUTOMATIC LIFETIME-FREE SQLITE DATABASE ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS incidents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lat REAL,
            lng REAL,
            category TEXT,
            description TEXT,
            severity TEXT,
            ingested_at TEXT
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
    
    incidents = []
    for r in rows:
        incidents.append({
            "event": "new_incident",
            "incident": {"lat": r[0], "lng": r[1], "category": r[2], "description": r[3], "severity": r[4]}
        })
    return incidents

def database_auto_scrub():
    """Removes all tactical data older than 24 hours to prevent cluttering routing calculations"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    time_threshold = (datetime.utcnow() - timedelta(hours=24)).isoformat()
    cursor.execute("DELETE FROM incidents WHERE ingested_at < ?", (time_threshold,))
    deleted_count = cursor.rowcount
    conn.commit()
    conn.close()
    if deleted_count > 0:
        print(f"🧹 DATABASE AUTO-SCRUB: Cleared {deleted_count} expired threat profiles (>24h).")

# --- DATA SOURCE FEEDS ---
NEWS_FEEDS = ["https://www.france24.com/en/rss", "https://www.rfi.fr/en/france/rss"]
TRANSIT_FEED = "https://www.asf-en-direct.fr/rss/trafic-ratp.xml" 

# Hyper-Local Parisian Micro-Feeds
MICRO_FEEDS = [
    "https://www.leparisien.fr/paris-75/rss.xml", 
    "https://www.lefigaro.fr/paris/rss.xml"       
]

# --- CORE INTEL PROCESSING ENGINE ---
async def process_raw_report(title, description, source_type, source_name):
    clean_text = re.sub(r'<[^>]+>', ' ', title + " " + description).strip()
    
    # 1. Duplicate check
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM incidents WHERE description LIKE ?", (f"%{clean_text[:20]}%",))
    exists = cursor.fetchone()[0]
    conn.close()
    
    if exists or len(clean_text) < 10:
        return

    # 2. KEYWORD SHIELD: Pre-filter text to protect free tier daily limits
    security_keywords = [
        "protest", "manifestation", "strike", "grève", "riot", "émeute", 
        "police", "gendarmerie", "pompiers", "attack", "agression", 
        "stolen", "vol", "cambriolage", "accident", "choc", "blessé",
        "security", "sécurité", "suspect", "arrest", "interpellé",
        "transit", "trafic", "métro", "metro", "rer", "bus", "tram", 
        "fermé", "closed", "suspendu", "incident", "panne", "retard"
    ]
    
    combined_lower = (title + " " + description).lower()
    has_keyword = any(keyword in combined_lower for keyword in security_keywords)
    
    if not has_keyword:
        return

    # 3. AI Analysis Block
    print(f"🧠 AI Analyzing {source_type} [{source_name}]: {title[:50]}...")

    prompt = f"""
    Analyze this raw live update for an urban tactical operations map:
    Title: "{title}"
    Details: "{description}"
    Source Category: {source_type}

    CRITICAL RULES:
    1. Determine if this event marks a safety risk, strike, civil unrest, crime, or active transit disruption inside Paris or the Île-de-France region. If it's unrelated to Paris urban safety/infrastructure, set "relevant_to_paris" to false.
    2. Extract the exact Paris street, landmark, metro line, or train station mentioned. Lookup and provide its precise latitude and longitude.
    3. If it is an active disruption or protest relevant to Paris but no exact street/station is named, set "exact_location_found" to false and use default center coordinates (48.8566, 2.3522).
    4. Map "category" strictly to: PROTEST, DAMAGE, CRIME, TRANSIT, or SECURITY. 
    5. Determine "severity" strictly as: low, medium, or high.

    Respond ONLY with this valid JSON template. No markdown wrappers, no comments, no extra text:
    {{"relevant_to_paris": true, "exact_location_found": true, "lat": 48.8566, "lng": 2.3522, "severity": "medium", "category": "TRANSIT"}}
    """

    try:
        response = await asyncio.to_thread(ai_model.generate_content, prompt)
        raw_json = response.text.strip()
        if "{" in raw_json: 
            raw_json = raw_json[raw_json.find("{"):raw_json.rfind("}")+1]
        
        ai_data = json.loads(raw_json)
        
        if not ai_data.get("relevant_to_paris", False):
            return
            
        lat = float(ai_data.get("lat", PARIS_CENTER_LAT))
        lng = float(ai_data.get("lng", PARIS_CENTER_LNG))
        
        if not ai_data.get("exact_location_found", False):
            lat += random.uniform(-0.005, 0.005)
            lng += random.uniform(-0.005, 0.005)

        incident_data = {
            "lat": lat, 
            "lng": lng, 
            "category": ai_data.get("category", source_type).upper(),
            "description": f"<b>🚨 {source_type} ALERT: {title}</b><br><br>{description[:160]}...<br><br>Source: {source_name}",
            "severity": ai_data.get("severity", "medium").lower()
        }
        
        store_incident(incident_data)
        
        payload = {"event": "new_incident", "incident": incident_data}
        await manager.broadcast(payload)
        print(f"📍 REAL-TIME AI DROP: [{incident_data['category']}] saved & dispatched.")
        
    except Exception as e:
        print(f"⚠️ Intel processing skipped: {e}")

# --- TRACKER LOOPS ---
async def master_intelligence_loop():
    database_auto_scrub()

    print("📰 Scraping Global Macro News Feeds...")
    for feed_url in NEWS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            if feed.entries:
                for entry in reversed(feed.entries[:5]):
                    await process_raw_report(
                        getattr(entry, 'title', ''), 
                        getattr(entry, 'description', ''), 
                        "NEWS", 
                        "France24" if "france24" in feed_url else "RFI"
                    )
        except: pass

    print("🔍 Scraping Hyper-Local Paris Micro-Feeds...")
    for feed_url in MICRO_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            if feed.entries:
                for entry in reversed(feed.entries[:5]):
                    await process_raw_report(
                        getattr(entry, 'title', ''), 
                        getattr(entry, 'description', ''), 
                        "LOCAL INTEL", 
                        "LeParisien" if "leparisien" in feed_url else "LeFigaro"
                    )
        except: pass

    print("🚇 Scraping Local Paris Transit Disruption Streams...")
    try:
        transit_feed = feedparser.parse(TRANSIT_FEED)
        if transit_feed.entries:
            for entry in reversed(transit_feed.entries[:10]):
                await process_raw_report(
                    getattr(entry, 'title', ''), 
                    getattr(entry, 'description', ''), 
                    "TRANSIT", 
                    "RATP Live Traffic"
                )
    except: pass
    print("✅ System Core Scrapers Synchronized.")

async def background_task():
    await master_intelligence_loop()
    while True:
        await asyncio.sleep(180) # Scans every 3 minutes to optimize pipeline health
        await master_intelligence_loop()

# --- FASTAPI LIFESPAN ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Booting Multi-Source AI OSINT Dashboard Server...")
    init_db() 
    asyncio.create_task(background_task())
    yield

# --- FASTAPI APP ---
app = FastAPI(title="Grey Lane Backend", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.get("/")
def read_root(): return {"status": "Online"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
