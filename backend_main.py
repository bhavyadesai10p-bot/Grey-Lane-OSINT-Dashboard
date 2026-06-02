import os
import json
import asyncio
import random
import feedparser
import re
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import google.generativeai as genai
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# --- AI SETUP ---
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
ai_model = genai.GenerativeModel('gemini-3.1-flash-lite')

cached_incidents = []

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        for incident in cached_incidents:
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

# --- PARIS HUB GEOLOCATION DEFINITION ---
PARIS_CENTER_LAT = 48.8566
PARIS_CENTER_LNG = 2.3522

# --- SYSTEM 1: TELEGRAM NATIVE LISTENER (REAL-TIME PARIS) ---
api_id = int(os.environ.get("TELEGRAM_API_ID", 0))
api_hash = os.environ.get("TELEGRAM_API_HASH", "")
session_string = os.environ.get("TELEGRAM_SESSION", "")

telegram_client = TelegramClient(StringSession(session_string), api_id, api_hash)
TELEGRAM_CHANNELS = ["BFMTV_news", "infotrafic_idf", "greylane_test_paris"]

@telegram_client.on(events.NewMessage(chats=TELEGRAM_CHANNELS))
async def telegram_handler(event):
    global cached_incidents
    raw_text = event.message.text
    if not raw_text or len(raw_text) < 10:
        return
        
    clean_text = raw_text.replace('\n', ' ').strip()
    channel_name = event.chat.username if event.chat else "Paris Intel"

    prompt = f"""
    Analyze this tactical intelligence report: "{clean_text}"
    
    CRITICAL RULES:
    1. Determine if this event is happening in Paris or the Île-de-France region. If it is NOT related to Paris/France at all, set "is_paris" to false.
    2. Look for street names (e.g., Rue de Rivoli), shops, landmarks (e.g., Louvre), or Arrondissements (e.g., 10ème).
    3. If a specific street/shop/landmark is found, provide its exact lat/lng.
    4. If it is a general Paris alert with NO specific address, set "exact_location_found" to false and use the default Paris coordinates (48.8566, 2.3522).
    
    Respond ONLY with this valid JSON format:
    {{"is_paris": true, "exact_location_found": true, "lat": 48.8566, "lng": 2.3522, "severity": "high", "category": "PROTEST"}}
    
    Categories can be: PROTEST, ROBBERY, TRAFFIC, SECURITY, or GENERAL.
    """
    
    try:
        response = await asyncio.to_thread(ai_model.generate_content, prompt)
        raw_json_text = response.text.strip()
        if "{" in raw_json_text and "}" in raw_json_text:
            raw_json_text = raw_json_text[raw_json_text.find("{"):raw_json_text.rfind("}")+1]
        ai_data = json.loads(raw_json_text)
        
        if not ai_data.get("is_paris", True):
            return

        lat = float(ai_data.get("lat", PARIS_CENTER_LAT))
        lng = float(ai_data.get("lng", PARIS_CENTER_LNG))
        
        if not ai_data.get("exact_location_found", True):
            lat += random.uniform(-0.008, 0.008)
            lng += random.uniform(-0.008, 0.008)

        category = ai_data.get("category", "TELEGRAM INTEL").upper()
        severity = ai_data.get("severity", "medium")
    except Exception as e:
        print(f"Paris AI Parse Error: {e}")
        return

    incident = {
        "event": "new_incident",
        "incident": {
            "lat": lat,
            "lng": lng,
            "category": category,
            "description": f"<b>🚨 LIVE PARIS INTEL</b><br>{clean_text[:160]}...<br><br>Source: t.me/{channel_name}",
            "severity": severity
        }
    }
    
    cached_incidents.append(incident)
    if len(cached_incidents) > 200: cached_incidents.pop(0) 
    await manager.broadcast(incident)

# --- SYSTEM 2: TRADITIONAL NEWS SCRAPER (POLLING PARIS ONLY) ---
FEED_URLS = [
    "https://www.france24.com/en/rss",         
    "https://www.rfi.fr/en/france/rss",        
    "https://www.thelocal.fr/feed"
]

async def unified_intelligence_scraper():
    global cached_incidents
    for feed_url in FEED_URLS:
        try:
            feed = feedparser.parse(feed_url)
            if feed.entries:
                for entry in reversed(feed.entries[:15]): 
                    raw_content = getattr(entry, 'title', '') + " " + getattr(entry, 'description', '')
                    clean_text = re.sub(r'<[^>]+>', ' ', raw_content).strip()
                    already_exists = any(inc["incident"]["description"].find(clean_text[:20]) != -1 for inc in cached_incidents)
                    
                    if not already_exists and len(clean_text) > 10:
                        prompt = f"""
                        Analyze this news report: "{clean_text}"
                        If this event is not explicitly happening in or directly affecting Paris/Île-de-France, set "is_paris" to false.
                        If it is in Paris, extract any granular landmarks, roads, or storefronts.
                        
                        Respond ONLY with this JSON layout:
                        {{"is_paris": true, "exact_location_found": true, "lat": 48.8566, "lng": 2.3522, "severity": "medium", "category": "SECURITY"}}
                        """
                        try:
                            response = await asyncio.to_thread(ai_model.generate_content, prompt)
                            raw_json_text = response.text.strip()
                            if "{" in raw_json_text and "}" in raw_json_text:
                                raw_json_text = raw_json_text[raw_json_text.find("{"):raw_json_text.rfind("}")+1]
                            ai_data = json.loads(raw_json_text)
                            
                            if not ai_data.get("is_paris", False):
                                continue 

                            lat = float(ai_data.get("lat", PARIS_CENTER_LAT))
                            lng = float(ai_data.get("lng", PARIS_CENTER_LNG))
                            
                            if not ai_data.get("exact_location_found", True):
                                lat += random.uniform(-0.01, 0.01)
                                lng += random.uniform(-0.01, 0.01)
                                
                            severity = ai_data.get("severity", "medium")
                            category = ai_data.get("category", "LIVE AI INTEL").upper()
                        except: 
                            continue

                        source_display = "France24" if "france24" in feed_url else "RFI" if "rfi" in feed_url else "The Local"
                        
                        incident = {
                            "event": "new_incident",
                            "incident": {
                                "lat": lat, "lng": lng, "category": category,
                                "description": f"<b>{clean_text[:160]}...</b><br><br>Source: {source_display} Paris Desk",
                                "severity": severity
                            }
                        }
                        cached_incidents.append(incident)
                        if len(cached_incidents) > 200: cached_incidents.pop(0) 
                        await manager.broadcast(incident)
                        await asyncio.sleep(4) 
        except Exception as e:
            print(f"Scraper Error for {feed_url}: {e}")

async def background_task():
    await unified_intelligence_scraper()
    while True:
        await asyncio.sleep(60) 
        await unified_intelligence_scraper()

# --- NON-BLOCKING DEPLOYMENT FAIL-SAFE ---
async def telegram_connect_task():
    print("🚀 Background connection task to Telegram Matrix started...")
    connected = False
    attempts = 0
    max_attempts = 5
    
    while not connected and attempts < max_attempts:
        try:
            attempts += 1
            print(f"🔄 Connection attempt {attempts}/{max_attempts} to Telegram Matrix...")
            
            if telegram_client.is_connected():
                await telegram_client.disconnect()
                await asyncio.sleep(2)
                
            await telegram_client.start()
            connected = True
            print("官 Grey Lane Telethon Wire established successfully!")
        except Exception as e:
            print(f"⚠️ Session conflict detected: {e}")
            print("Waiting 10 seconds to retry...")
            await asyncio.sleep(10)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # This fires tasks instantly without waiting, letting FastAPI open its port right away
    asyncio.create_task(telegram_connect_task())
    asyncio.create_task(background_task())
    yield
    
    try:
        await telegram_client.disconnect()
        print("🛑 Grey Lane Wire disconnected cleanly.")
    except:
        pass

# --- FASTAPI APP INITIALIZATION ---
app = FastAPI(title="Grey Lane OSINT Backend", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"status": "Grey Lane Paris OSINT Command System is Online."}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
