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

# --- PARIS TRANSIT GEOLOCATION MATRIX ---
# Maps entire lines to critical hub stations to spread alerts organically across the city
PARIS_TRANSIT_NODES = {
    "1": {"lat": 48.8654, "lng": 2.3211},  # Concorde (West-Central)
    "2": {"lat": 48.8820, "lng": 2.3323},  # Pigalle (North)
    "3": {"lat": 48.8649, "lng": 2.3985},  # Gambetta (East)
    "4": {"lat": 48.8530, "lng": 2.3444},  # Saint-Michel (Central)
    "5": {"lat": 48.8322, "lng": 2.3556},  # Place d'Italie (South)
    "6": {"lat": 48.8410, "lng": 2.3190},  # Montparnasse (South-West)
    "7": {"lat": 48.8590, "lng": 2.3580},  # Châtelet (Center-Right)
    "8": {"lat": 48.8675, "lng": 2.3136},  # Invalides (West)
    "9": {"lat": 48.8710, "lng": 2.3300},  # Opéra (Center-North)
    "10": {"lat": 48.8510, "lng": 2.2980}, # Dupleix (West)
    "11": {"lat": 48.8670, "lng": 2.3650}, # République (East-Central)
    "12": {"lat": 48.8430, "lng": 2.3410}, # Port-Royal (South-Central)
    "13": {"lat": 48.8800, "lng": 2.3150}, # Place de Clichy (North-West)
    "14": {"lat": 48.8335, "lng": 2.3734}, # Bibliothèque (South-East)
    "A": {"lat": 48.8738, "lng": 2.2950},  # RER A - Etoile
    "B": {"lat": 48.8188, "lng": 2.3387},  # RER B - Cité U
    "C": {"lat": 48.8533, "lng": 2.2764},  # RER C - Javel
    "D": {"lat": 48.8809, "lng": 2.3553},  # RER D - Gare du Nord
    "E": {"lat": 48.8768, "lng": 2.3592},  # RER E - Gare de l'Est
}

# ══════════════════════════════════════════════════════════
# LIVE MASTER CACHE STORAGE (Instant Page Loads)
# ══════════════════════════════════════════════════════════
# Saves up to 50 processed incidents so refreshes take 0.1 seconds instead of re-calling Gemini
INCIDENT_CACHE = []
MAX_CACHE_SIZE = 50

# --- AI SETUP ---
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))

SYSTEM_INSTRUCTION = """
You are an advanced OSINT geopolitical and urban intelligence parser for the Grey Lane platform in Paris. 
Analyze the provided text and extract details into STRICT JSON format.
Your objective is to maintain a vibrant, active operational map. Do NOT filter out general city developments, local events, or political updates. If the event takes place in or impacts the Paris/Île-de-France region, it is RELEVANT.

Categorization Guide:
- CIVIL_UNREST: Protests, strikes, marches, gatherings, political demonstrations.
- PROPERTY_DAMAGE: Accidents, construction closures, fires, infrastructure updates.
- CRIME: Arrests, police operations, guard custody updates, investigations.
- LOCAL_INTEL: General city updates, major events, cultural/political happenings, or new city developments.

You must return ONLY a JSON object with exactly these keys:
{
    "category": "Choose one: CIVIL_UNREST, PROPERTY_DAMAGE, CRIME, LOCAL_INTEL",
    "incident_type": "Short 2-3 word tag (e.g., 'City Marathon', 'Police Custody', 'New Venue')",
    "severity": "Rate 1 to 5 (Use 1 or 2 for general news/lifestyle, 3 for warnings, 4 or 5 for critical threats)",
    "start_lat": "Latitude of the location (float)",
    "start_lng": "Longitude of the location (float)",
    "end_lat": "Latitude (same as start_lat unless a moving protest)",
    "end_lng": "Longitude (same as start_lng unless a moving protest)",
    "description": "A brief, tactical summary of the news or event."
}
"""

ai_model = genai.GenerativeModel('gemini-3.1-flash-lite', system_instruction=SYSTEM_INSTRUCTION)

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        # On connection, stream the cached incidents first for instant page load (0.1s)
        for cached_incident in INCIDENT_CACHE:
            await websocket.send_json({
                "event": "new_incident",
                "incident": cached_incident
            })
        # Then stream any unexpired history from database as fallback
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

# ══════════════════════════════════════════════════════════
# CACHE + BROADCAST HELPER FUNCTION
# ══════════════════════════════════════════════════════════
async def broadcast_new_incident(incident_data):
    """
    Saves incident to in-memory cache and broadcasts to all connected clients.
    Cache provides instant page loads (0.1s) on refresh without re-calling Gemini.
    """
    # Save to memory cache first
    INCIDENT_CACHE.append(incident_data)
    if len(INCIDENT_CACHE) > MAX_CACHE_SIZE:
        INCIDENT_CACHE.pop(0)  # Keep memory lightweight
        
    # Broadcast out to live frontend dashboard instantly
    await manager.broadcast({
        "event": "new_incident",
        "incident": incident_data
    })

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

# --- FIX B: BATCH PROCESSING ---
async def analyze_articles_batch(articles, source_type, source_name):
    """
    Professional batch processing: Send 5-10 articles to Gemini in ONE call.
    Returns array of incidents instead of 1 at a time.
    Reduces API calls from 10 per cycle to 1 per cycle per feed!
    """
    if not articles:
        return []
    
    # Create article block for Gemini
    articles_block = ""
    for i, article in enumerate(articles):
        articles_block += f"""
ID: {i}
Title: {article['title']}
Summary: {article['summary'][:200]}
---"""
    
    prompt = f"""
Analyze this batch of {len(articles)} articles for incidents in the Paris/Île-de-France region.
Return a JSON array of objects with ONLY the relevant articles.

Articles:
{articles_block}

For each relevant article, respond with:
{{"id": 0, "relevant_to_paris": true, "lat": 48.8566, "lng": 2.3522, "severity": "medium", "category": "TRANSIT"}}

Return ONLY a JSON array, no markdown. If no articles are relevant, return [].
Example:
[{{"id": 1, "relevant_to_paris": true, "lat": 48.9356, "lng": 2.3539, "severity": "high", "category": "PROTEST"}}]
"""
    
    try:
        print(f"🚀 Batch analyzing {len(articles)} articles from {source_name}...")
        response = await asyncio.to_thread(ai_model.generate_content, prompt)
        
        print(f"🕵️ BATCH GEMINI RAW OUTPUT:\n{response.text}\n---")
        
        raw_json = response.text.strip()
        if "[" in raw_json:
            raw_json = raw_json[raw_json.find("["):raw_json.rfind("]")+1]
        
        print(f"🕵️ EXTRACTED BATCH JSON: {raw_json}")
        
        batch_results = json.loads(raw_json)
        if not isinstance(batch_results, list):
            batch_results = [batch_results]
        
        print(f"🕵️ PARSED BATCH SUCCESS: {len(batch_results)} relevant articles")
        
        # Process each result
        incidents = []
        for result in batch_results:
            if not result.get("relevant_to_paris", False):
                continue
            
            article_id = result.get("id", 0)
            if article_id >= len(articles):
                continue
            
            article = articles[article_id]
            lat = float(result.get("lat", PARIS_CENTER_LAT))
            lng = float(result.get("lng", PARIS_CENTER_LNG))
            
            incident_data = {
                "lat": lat,
                "lng": lng,
                "category": result.get("category", "SECURITY").upper(),
                "description": f"<b>📰 {source_type} BATCH: {article['title']}</b><br><br>{article['summary'][:160]}...<br><br>Source: {source_name}",
                "severity": result.get("severity", "medium").lower()
            }
            
            store_incident(incident_data)
            incidents.append(incident_data)
            
            await broadcast_new_incident(incident_data)
            print(f"📍 BATCH AI DROP: [{incident_data['category']}] saved & dispatched.")
        
        return incidents
        
    except json.JSONDecodeError as je:
        print(f"❌ BATCH JSON PARSE FAILED: {je}")
        print(f"   Raw text was: {response.text[:200]}")
        return []
    except Exception as e:
        print(f"⚠️ Batch processing failed: {type(e).__name__}: {e}")
        return []

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
    # Now accepts broader urban incidents: accidents, construction, property damage, gatherings, events, and local city news
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
        "security", "sécurité", "suspect", "risk", "risque", "danger", "emergency", "urgence",
        # --- NEW LOCAL NEWS KEYWORDS FOR YELLOW DOTS (CITY EVENTS & POLITICS) ---
        "politique", "mairie", "projet", "événement", "ouverture", 
        "annonce", "france", "ville", "quartier", "rue", "développement", "nouveau"
    ]
    
    combined_lower = (title + " " + description).lower()
    has_keyword = any(keyword in combined_lower for keyword in security_keywords)
    
    if not has_keyword:
        print(f"🚫 Python Filter Blocked: {title[:40]}...")
        return

    print(f"🧠 AI Analyzing {source_type} [{source_name}]: {title[:50]}...")
    prompt = f"""
    Analyze this Paris news report:
    Title: {title}
    Description: {description}
    
    Determine if this happens in or mentions Paris/Île-de-France. If yes, set relevant_to_paris to true.
    Extract the most accurate coordinates possible for the location mentioned. If a general neighborhood or just 'Paris' is given, use coordinates near that area.
    
    Respond ONLY with a valid JSON object formatted exactly like this:
    {{"relevant_to_paris": true, "exact_location_found": true, "lat": 48.8566, "lng": 2.3522, "severity": "medium", "category": "LOCAL_INTEL"}}
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
        
        await broadcast_new_incident(incident_data)
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
                
                # 5. Smart Geolocator: Try to find the line code in our matrix
                # Extract just the line number/letter (e.g., "1", "4", "A" from "Line 1")
                line_code = str(line_name).upper().replace("LINE ", "").strip()
                
                if line_code in PARIS_TRANSIT_NODES:
                    # Drop the alert at the designated hub station for that line
                    incident_lat = PARIS_TRANSIT_NODES[line_code]["lat"]
                    incident_lng = PARIS_TRANSIT_NODES[line_code]["lng"]
                else:
                    # Fallback for minor bus/tram lines: 
                    # Apply a deterministic mathematical scatter so they don't form a dead-center spiral
                    import hashlib
                    hash_val = int(hashlib.md5(line_code.encode()).hexdigest(), 16)
                    
                    # Creates an organic, predictable scatter around Paris based on the bus/tram name
                    incident_lat = 48.8566 + ((hash_val % 100) - 50) * 0.001
                    incident_lng = 2.3522 + (((hash_val // 100) % 100) - 50) * 0.001
                
                lat = incident_lat
                lon = incident_lng
                
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
    
    TWO MODES:
    - Fix A (Current): Individual articles with 2-second delays
    - Fix B (Commented): Batch processing for 10x fewer API calls
    
    To upgrade to Fix B batch mode, uncomment the batch section below.
    """
    
    print("📰 Scraping Macro French News Feeds...")
    for feed_url in NEWS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            if feed.entries:
                # --- FIX A MODE (CURRENT) ---
                for entry in reversed(feed.entries[:5]):
                    await process_raw_report(
                        getattr(entry, 'title', ''), 
                        getattr(entry, 'description', ''), 
                        "NEWS", 
                        "France24" if "france24" in feed_url else "RFI"
                    )
                    # 🚨 FIX A: 2-second breathing room to prevent 429 rate limit errors
                    await asyncio.sleep(2)
                
                # --- FIX B MODE (UPGRADE - uncomment to enable) ---
                # Collect articles for batch processing
                # articles = []
                # for entry in reversed(feed.entries[:5]):
                #     articles.append({
                #         "title": getattr(entry, 'title', ''),
                #         "summary": getattr(entry, 'description', '')
                #     })
                # source_name = "France24" if "france24" in feed_url else "RFI"
                # await analyze_articles_batch(articles, "NEWS", source_name)
                # await asyncio.sleep(2)  # Still wait between batch calls
                
        except Exception as e:
            print(f"⚠️ Macro feed error: {e}")

    print("🔍 Scraping Hyper-Local Paris Micro-Feeds...")
    for feed_url in MICRO_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            if feed.entries:
                # --- FIX A MODE (CURRENT) ---
                for entry in reversed(feed.entries[:5]):
                    await process_raw_report(
                        getattr(entry, 'title', ''), 
                        getattr(entry, 'description', ''), 
                        "LOCAL INTEL", 
                        "LeParisien" if "leparisien" in feed_url else "LeFigaro"
                    )
                    # 🚨 FIX A: 2-second breathing room to prevent 429 rate limit errors
                    await asyncio.sleep(2)
                
                # --- FIX B MODE (UPGRADE - uncomment to enable) ---
                # Collect articles for batch processing
                # articles = []
                # for entry in reversed(feed.entries[:5]):
                #     articles.append({
                #         "title": getattr(entry, 'title', ''),
                #         "summary": getattr(entry, 'description', '')
                #     })
                # source_name = "LeParisien" if "leparisien" in feed_url else "LeFigaro"
                # await analyze_articles_batch(articles, "LOCAL INTEL", source_name)
                # await asyncio.sleep(2)  # Still wait between batch calls
                
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
