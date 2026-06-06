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

INCIDENT ACCEPTANCE RULES:
Accept any local news event that impacts daily urban life, safety, or logistics in the region.
Do NOT restrict yourself to violent crises. If an event details:
- An arrest or police activity
- A vehicle accident or traffic incident
- Property damage or vandalism
- Localized urban construction or roadwork
- Regional gatherings, protests, or demonstrations
- Transit disruptions or service changes
- Industrial incidents or workplace emergencies
- Public health or environmental issues
...then mark 'relevant_to_paris': true.

The map should reflect the full spectrum of urban activity, not just crises.

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
    # Now accepts broader urban incidents: accidents, construction, property damage, gatherings, etc.
    security_keywords = [
        # Protests & civil unrest
        "protest", "manifestation", "strike", "grève", "riot", "émeute", "march", "défilé",
        # Law enforcement & crime
        "police", "gendarmerie", "pompiers", "attack", "agression", "arrest", "interpellé",
        "stolen", "vol", "cambriolage", "robbery", "braquage", "crime", "délit",
        # Accidents & incidents
        "accident", "crash", "collision", "choc", "blessé", "injured", "fatality", "mort",
        "incident", "emergency", "secours", "urgence", "sapeurs-pompiers",
        # Property damage & vandalism
        "damage", "dégât", "destruction", "vandal", "dégradation", "broken", "cassé",
        "fire", "incendie", "explosion", "effondrement", "collapse",
        # Traffic & transit
        "transit", "trafic", "métro", "metro", "rer", "bus", "tram", "traffic", "congestion",
        "fermé", "closed", "suspendu", "suspended", "delayed", "retard", "panne", "breakdown",
        # Construction & urban works
        "construction", "travaux", "roadwork", "chantier", "road closure", "fermeture",
        "maintenance", "entretien", "repair", "réparation",
        # Gatherings & events
        "gathering", "assembly", "rassemblement", "event", "événement", "festival",
        # General urban terms (keep map active)
        "paris", "île-de-france", "region", "région", "incident", "alert", "alerte",
        "security", "sécurité", "suspect", "risk", "risque", "danger", "emergency", "urgence"
    ]
    
    combined_lower = (title + " " + description).lower()
    has_keyword = any(keyword in combined_lower for keyword in security_keywords)
    
    if not has_keyword:
        return

    # 3. AI Analysis Block
    print(f"🧠 AI Analyzing {source_type} [{source_name}]: {title[:50]}...")

    prompt = f"""
    Analyze this urban incident report from the Paris/Île-de-France region:
    Title: "{title}"
    Description: "{description}"
    Source Type: {source_type}

    Determine if this event impacts daily urban life, safety, logistics, or infrastructure in the region.
    Accept: accidents, arrests, property damage, construction, transit issues, gatherings, etc.
    Do NOT restrict to violent crises only.
    
    Extract the precise location (latitude/longitude) if mentioned. If no exact location is named, 
    use Paris center (48.8566, 2.3522) and set exact_location_found to false.
    
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

# --- SUBURBAN COORDINATE FALLBACKS ---
# Map transit lines/zones to their appropriate suburban department coordinates
SUBURBAN_ZONES = {
    # Department 92 (Hauts-de-Seine): La Défense, Nanterre, Boulogne
    92: {"name": "La Défense / Nanterre", "lat": 48.8924, "lng": 2.2362},
    # Department 93 (Seine-Saint-Denis): Saint-Denis, Roissy-CDG, Aulnay
    93: {"name": "Saint-Denis / Roissy", "lat": 48.9356, "lng": 2.3539},
    # Department 94 (Val-de-Marne): Créteil, Vitry, Champigny
    94: {"name": "Créteil / Vitry", "lat": 48.7831, "lng": 2.4534},
    # Department 95 (Val-d'Oise): Cergy, Pontoise, Roissy North
    95: {"name": "Cergy / Pontoise", "lat": 49.0187, "lng": 2.0669},
    # Department 77 (Seine-et-Marne): Meaux, Fontainebleau, East suburbs
    77: {"name": "Meaux / Fontainebleau", "lat": 48.9564, "lng": 2.8837},
    # Department 78 (Yvelines): Versailles, Saint-Germain, West suburbs
    78: {"name": "Versailles / Saint-Germain", "lat": 48.8041, "lng": 2.1007},
    # Department 91 (Essonne): Évry, Corbeil, South suburbs
    91: {"name": "Évry / Corbeil", "lat": 48.6292, "lng": 2.4413},
    # Central Paris (75) - fallback if department unknown
    75: {"name": "Central Paris", "lat": 48.8566, "lng": 2.3522},
}

def infer_department_from_line(line_name, station_name):
    """
    Intelligently map transit lines/stations to their department zone.
    Returns the department code (75-95) or None if unknown.
    """
    combined = f"{line_name} {station_name}".upper()
    
    # RER & Train line mappings
    rer_dept_map = {
        "RER A": 92,  # Passes through La Défense
        "RER B": 93,  # Passes through Roissy
        "RER C": 75,  # South/Central
        "RER D": 93,  # Saint-Denis direction
        "RER E": 75,  # Central
        "RER H": 75,  # Central
        "TRANSILIEN": 77,  # East/Southeast
        "TER": 77,  # Regional trains - east
    }
    
    # Suburban station keywords → department
    station_dept_map = {
        "DÉFENSE": 92, "NANTERRE": 92, "BOULOGNE": 92, "LEVALLOIS": 92,
        "DENIS": 93, "ROISSY": 93, "AULNAY": 93, "MONTREUIL": 93,
        "CRÉTEIL": 94, "VITRY": 94, "CHAMPIGNY": 94, "IVRY": 94,
        "CERGY": 95, "PONTOISE": 95, "GONESSE": 95,
        "VERSAILLES": 78, "SAINT-GERMAIN": 78, "MARLY": 78,
        "ÉVRY": 91, "CORBEIL": 91, "ESSONNE": 91,
        "MEAUX": 77, "FONTAINEBLEAU": 77, "SEINE-ET-MARNE": 77,
    }
    
    # Check RER/Line patterns
    for line_pattern, dept in rer_dept_map.items():
        if line_pattern in combined:
            return dept
    
    # Check station keywords
    for station_keyword, dept in station_dept_map.items():
        if station_keyword in combined:
            return dept
    
    return None  # Unknown - will use default Paris

# --- IDFM OFFICIAL TRANSIT API ---
def fetch_idfm_transit_status():
    """Queries the official IDFM API, parses disruptions, and saves them to the database.
    Now intelligently maps suburban transit to appropriate coordinates instead of centralizing."""
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
                
                # 3. Extract line/route info for intelligent department mapping
                line_name = ""
                station_name = ""
                impacted = dist.get("impacted_objects", [])
                
                if impacted:
                    pt_object = impacted[0].get("pt_object", {})
                    
                    # Try to get line information
                    if "line" in pt_object:
                        line_name = pt_object["line"].get("name", "")
                    
                    # Try to get stop/station information
                    stop_area = pt_object.get("stop_area", {})
                    if stop_area:
                        station_name = stop_area.get("name", "")
                
                # 4. Try to extract actual coordinates from API
                lat, lon = None, None
                if impacted:
                    pt_object = impacted[0].get("pt_object", {})
                    stop_area = pt_object.get("stop_area", {})
                    coord = stop_area.get("coord", {})
                    if coord and "lat" in coord and "lon" in coord:
                        lat = float(coord["lat"])
                        lon = float(coord["lon"])
                
                # 5. Intelligent fallback: infer department from line/station
                if lat is None or lon is None:
                    inferred_dept = infer_department_from_line(line_name, station_name)
                    if inferred_dept and inferred_dept in SUBURBAN_ZONES:
                        zone = SUBURBAN_ZONES[inferred_dept]
                        lat = zone["lat"]
                        lon = zone["lng"]
                        print(f"🗺️ Mapped '{line_name}' to {zone['name']} ({inferred_dept})")
                    else:
                        # Ultimate fallback: default to central Paris
                        lat = PARIS_CENTER_LAT
                        lon = PARIS_CENTER_LNG
                        print(f"⚠️ Could not infer location for '{line_name}' at '{station_name}', using central Paris")
                
                # 6. Save to database
                incident_data = {
                    "lat": lat,
                    "lng": lon,
                    "category": category,
                    "description": f"[IDFM] {text_alert}<br><strong>Line:</strong> {line_name}",
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
async def scrape_transit_data():
    """
    Fetches real-time transit disruptions from RATP and IDFM.
    Runs first so transport updates hit the map instantly.
    """
    print("🚇 Polling IDFM Official Transit API...")
    fetch_idfm_transit_status()
    
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
    
    print("✅ Transit data synchronized.")

async def scrape_intelligence_feeds():
    """
    Fetches and analyzes macro news and hyper-local Paris intel.
    Uses AI to classify incidents and push them to the map.
    """
    print("📰 Scraping Macro French News Feeds...")
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
        except Exception as e:
            print(f"⚠️ Macro feed error: {e}")

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
        except Exception as e:
            print(f"⚠️ Micro feed error: {e}")
    
    print("✅ Intelligence feeds synchronized.")

async def intelligence_gathering_loop():
    """
    Background worker that handles all OSINT data fetching silently in the background,
    preventing any frontend timeouts or lag.
    
    Sequence:
    1. Clean old data (>24h)
    2. Scrape transit (fast, hits map instantly)
    3. Scrape intelligence feeds (AI analysis)
    4. Rest for 5 minutes before next cycle
    """
    while True:
        try:
            print("🚀 Starting background intelligence sweep...")
            
            # Clean old incidents from database first
            database_auto_scrub()
            
            # 1. Run transit first so transport updates hit the map instantly
            await scrape_transit_data()
            
            # 2. Run the heavier AI news analysis loops right after
            await scrape_intelligence_feeds()
            
            print("✅ Sweep complete. Resting for 5 minutes...")
            await asyncio.sleep(300)  # Wait 5 minutes before pulling new data
            
        except Exception as e:
            print(f"❌ Error in background intelligence worker: {type(e).__name__}: {e}")
            print("⏱️ Retrying in 60 seconds...")
            await asyncio.sleep(60)  # Wait 1 minute before retrying if something fails

# --- FASTAPI LIFESPAN & STARTUP ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Modern FastAPI startup/shutdown handler using lifespan context manager."""
    print("🚀 Booting Multi-Source AI OSINT Dashboard Server...")
    print("⚙️  Initializing SQLite database...")
    init_db()
    
    print("🧠 Launching background intelligence gathering loop (5-minute intervals)...")
    asyncio.create_task(intelligence_gathering_loop())
    print("🤖 Background intelligence worker successfully detached and running.")
    
    print("✅ Server ready! Website loads instantly. Scraping runs in background.")
    yield
    
    print("🛑 Shutting down server...")

# Alternative startup method (both approaches work)
# Uncomment to use classic @app.on_event approach instead of lifespan:
# @app.on_event("startup")
# async def startup_event():
#     asyncio.create_task(intelligence_gathering_loop())
#     print("🤖 Background intelligence worker detached.")

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
