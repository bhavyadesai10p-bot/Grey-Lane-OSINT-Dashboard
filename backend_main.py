import os
import json
import asyncio
import random
import feedparser
import re
import sqlite3
import requests
import time
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import google.generativeai as genai

# --- OFFICIAL IDFM TRANSIT API KEY ---
IDFM_API_KEY = "oHymtFY3DTr0tdSr5jghmDe1Qaxhxn9X"

# --- AI SETUP ---
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))

SYSTEM_INSTRUCTION = """
You are a senior OSINT analyst for the Paris metropolitan region.
Your primary task is to extract incident data from news and social media.

GEOGRAPHIC RELEVANCE RULES:
1. "Relevant to Paris" means the incident occurs ANYWHERE within the Île-de-France region (Department numbers 75, 77, 78, 91, 92, 93, 94, 95).
2. You MUST include incidents in all suburbs, industrial zones, and transport hubs surrounding Paris (e.g., Saint-Denis, Nanterre, La Défense, Créteil, Roissy-CDG).
3. Do NOT discard an article just because it doesn't mention the city center. If it happens in the suburbs, it IS relevant.

OUTPUT RULES:
- If relevant, return JSON with these exact fields: {"relevant_to_paris": true, "exact_location_found": true, "lat": <float>, "lng": <float>, "severity": "low/medium/high", "category": "PROTEST/DAMAGE/CRIME/TRANSIT/SECURITY", "description": "..."}
- If NOT relevant to the Île-de-France region, return {"relevant_to_paris": false}
- ALWAYS return valid JSON only. No markdown, no explanations, no extra text.
"""

ai_model = genai.GenerativeModel('gemini-3.1-flash-lite', system_instruction=SYSTEM_INSTRUCTION)

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

# --- STEP 1: UPDATED FEED URLs ---
MACRO_FEEDS = [
    "https://www.france24.com/fr/france/rss",                    # France24 (National/Societal)
    "https://www.lefigaro.fr/rss/figaro_actualite-france.xml",   # Le Figaro (French News)
]

MICRO_FEEDS = [
    "https://www.francetvinfo.fr/ile-de-france/paris/rss",       # France TV (Hyper-local Paris)
    "https://www.leparisien.fr/arc/outboundfeeds/rss/info/paris-75/" # Le Parisien's true raw XML pipe
]

TRANSIT_FEED = "https://www.asf-en-direct.fr/rss/trafic-ratp.xml" 

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
    Analyze this incident report:
    Title: "{title}"
    Description: "{description}"
    Source Type: {source_type}

    Extract the precise location (latitude/longitude) if mentioned. If no exact location is named, use the Paris center default (48.8566, 2.3522) and set exact_location_found to false.
    
    Classify severity as: low, medium, or high.
    Classify category as one of: PROTEST, DAMAGE, CRIME, TRANSIT, SECURITY.

    Respond ONLY with valid JSON. No markdown, no explanations:
    {{"relevant_to_paris": true, "exact_location_found": true, "lat": 48.8566, "lng": 2.3522, "severity": "medium", "category": "TRANSIT"}}
    """

    try:
        response = await asyncio.to_thread(ai_model.generate_content, prompt)
        
        # 🕵️ DIAGNOSTIC: Print exactly what Gemini returned
        print(f"🕵️ GEMINI RAW OUTPUT:\n{response.text}\n---")
        
        raw_json = response.text.strip()
        if "{" in raw_json: 
            raw_json = raw_json[raw_json.find("{"):raw_json.rfind("}")+1]
        
        print(f"🕵️ EXTRACTED JSON: {raw_json}")
        
        ai_data = json.loads(raw_json)
        print(f"🕵️ PARSED JSON SUCCESS: {ai_data}")
        
        if not ai_data.get("relevant_to_paris", False):
            print(f"⏭️ Skipped: Not relevant to Paris")
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
        
    except json.JSONDecodeError as je:
        print(f"❌ JSON PARSE FAILED: {je}")
        print(f"   Raw text was: {response.text[:200]}")
    except Exception as e:
        print(f"⚠️ Intel processing skipped: {type(e).__name__}: {e}")

# --- IDFM OFFICIAL TRANSIT API ---
def fetch_idfm_transit_status():
    """Queries the official IDFM API, parses disruptions, and saves them to the database."""
    print("🚇 Polling IDFM Official Transit API...")
    url = "https://prim.iledefrance-mobilites.fr/marketplace/v2/navitia/disruptions"
    headers = {"apiKey": IDFM_API_KEY}
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            disruptions = data.get("disruptions", [])
            print(f"✅ IDFM API Success: Analyzing {len(disruptions)} active transit alerts.")
            
            for dist in disruptions:
                # 1. Extract the core message
                messages = dist.get("messages", [])
                if not messages:
                    continue
                text_alert = messages[0].get("text", "Transit Disruption")
                
                # 2. Extract severity and set category
                severity = dist.get("severity", {}).get("name", "warning")
                category = "TRANSIT"
                
                # 3. Extract Coordinates
                lat, lon = 48.8566, 2.3522 # Default to Paris center
                impacted = dist.get("impacted_objects", [])
                if impacted:
                    pt_object = impacted[0].get("pt_object", {})
                    stop_area = pt_object.get("stop_area", {})
                    coord = stop_area.get("coord", {})
                    if coord and "lat" in coord and "lon" in coord:
                        lat = float(coord["lat"])
                        lon = float(coord["lon"])
                
                # --- NEW: THE MICRO-JITTER ---
                # If it used the default center coords, spread them out slightly!
                if lat == 48.8566 and lon == 2.3522:
                    lat += random.uniform(-0.01, 0.01)
                    lon += random.uniform(-0.01, 0.01)
                
                # 4. Save to your SQLite Database (skips Gemini entirely!)
                incident_data = {
                    "lat": lat,
                    "lng": lon,
                    "category": category,
                    "description": f"[IDFM] {text_alert}",
                    "severity": severity
                }
                store_incident(incident_data)
                
            return disruptions
        else:
            print(f"⚠️ IDFM API Error: {response.status_code}")
            return []
    except Exception as e:
        print(f"⚠️ IDFM Connection Error: {e}")
        return []

# --- TRACKER LOOPS ---
async def master_intelligence_loop():
    database_auto_scrub()

    print("📰 Scraping Macro French News Feeds...")
    for feed_url in MACRO_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            
            # 🚨 RADAR PING: See exactly how much data is coming in!
            print(f"📡 RADAR PING: Found {len(feed.entries)} articles at {feed_url}")
            
            if feed.entries:
                for entry in reversed(feed.entries[:10]):  # Top 10 articles
                    await process_raw_report(
                        getattr(entry, 'title', ''), 
                        getattr(entry, 'description', ''), 
                        "NEWS", 
                        "France24" if "france24" in feed_url else "Le Figaro"
                    )
        except Exception as e:
            print(f"⚠️ Macro feed error: {e}")

    # --- STEP 2: FIXED MICRO FEED SCRAPING ENGINE ---
    print("🔍 Scraping Hyper-Local Paris Micro-Feeds...")
    for feed_url in MICRO_FEEDS:
        try:
            parsed_feed = feedparser.parse(feed_url)
            
            # 🚨 RADAR PING: See exactly how much data is coming in!
            print(f"📡 RADAR PING: Found {len(parsed_feed.entries)} articles at {feed_url}")
            
            # Force the system to loop through the top 10 articles of EACH feed
            for entry in parsed_feed.entries[:10]: 
                headline = entry.get('title', '')
                summary = entry.get('summary', '')
                full_text = f"{headline} - {summary}"
                
                print(f"🧠 AI Analyzing LOCAL INTEL: {headline[:50]}...")
                
                # Send to process_raw_report (existing AI function)
                await process_raw_report(
                    headline,
                    summary,
                    "LOCAL INTEL",
                    "FranceTV" if "francetvinfo" in feed_url else "LeParisien"
                )
            
        except Exception as e:
            print(f"⚠️ Micro feed parse error ({feed_url}): {e}")

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
    except Exception as e:
        print(f"⚠️ Transit feed error: {e}")
    
    print("🔌 Querying Official IDFM Transit API...")
    fetch_idfm_transit_status()
    
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
