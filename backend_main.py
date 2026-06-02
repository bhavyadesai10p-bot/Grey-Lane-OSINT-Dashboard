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

# --- SYSTEM 1: TELEGRAM NATIVE LISTENER (REAL-TIME) ---
api_id = int(os.environ.get("TELEGRAM_API_ID", 0))
api_hash = os.environ.get("TELEGRAM_API_HASH", "")
session_string = os.environ.get("TELEGRAM_SESSION", "")

# Initialize the official Telegram client with your master key
telegram_client = TelegramClient(StringSession(session_string), api_id, api_hash)
TELEGRAM_CHANNELS = ["BFMTV_news", "infotrafic_idf"]

@telegram_client.on(events.NewMessage(chats=TELEGRAM_CHANNELS))
async def telegram_handler(event):
    global cached_incidents
    raw_text = event.message.text
    if not raw_text or len(raw_text) < 10:
        return
        
    clean_text = raw_text.replace('\n', ' ').strip()
    channel_name = event.chat.username if event.chat else "Telegram Intel"

    prompt = f"""
    Read this raw real-time Telegram intelligence: "{clean_text}"
    Identify the specific city, street, or landmark mentioned (assume Paris/France if vague).
    Give the exact latitude and longitude. Use central Paris (48.8566, 2.3522) if no location is found.
    Respond ONLY with a valid JSON object: {{"lat": 48.8566, "lng": 2.3522, "severity": "high"}}
    Determine severity (low, medium, high) based on if it mentions protests, police, accidents, etc.
    """
    
    lat, lng, severity = 48.8566 + random.uniform(-0.02, 0.02), 2.3522 + random.uniform(-0.02, 0.02), "medium"
    
    try:
        response = await asyncio.to_thread(ai_model.generate_content, prompt)
        raw_json_text = response.text.strip()
        if "{" in raw_json_text and "}" in raw_json_text:
            raw_json_text = raw_json_text[raw_json_text.find("{"):raw_json_text.rfind("}")+1]
        ai_data = json.loads(raw_json_text)
        lat = float(ai_data.get("lat", lat)) + random.uniform(-0.005, 0.005)
        lng = float(ai_data.get("lng", lng)) + random.uniform(-0.005, 0.005)
        severity = ai_data.get("severity", severity)
    except Exception as e:
        print(f"Telegram AI Parse Error: {e}")

    incident = {
        "event": "new_incident",
        "incident": {
            "lat": lat,
            "lng": lng,
            "category": "TELEGRAM INTEL",
            "description": f"<b>🚨 LIVE GROUND ALERT</b><br>{clean_text[:150]}...<br><br>Source: t.me/{channel_name}",
            "severity": severity
        }
    }
    
    cached_incidents.append(incident)
    if len(cached_incidents) > 30: cached_incidents.pop(0)
    await manager.broadcast(incident)

# --- SYSTEM 2: TRADITIONAL NEWS SCRAPER (POLLING) ---
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
                for entry in reversed(feed.entries[:2]): 
                    raw_content = getattr(entry, 'title', '') + " " + getattr(entry, 'description', '')
                    clean_text = re.sub(r'<[^>]+>', ' ', raw_content).strip()
                    already_exists = any(inc["incident"]["description"].find(clean_text[:20]) != -1 for inc in cached_incidents)
                    
                    if not already_exists and len(clean_text) > 10:
                        prompt = f"""
                        Read this raw news intelligence: "{clean_text}"
                        Identify the specific city, street, or landmark mentioned (assume Paris/France if vague).
                        Give exact latitude and longitude. Default to central Paris (48.8566, 2.3522).
                        Respond ONLY with a valid JSON object: {{"lat": 48.8566, "lng": 2.3522, "severity": "high"}}
                        Determine severity (low, medium, high).
                        """
                        lat, lng, severity = 48.8566 + random.uniform(-0.02, 0.02), 2.3522 + random.uniform(-0.02, 0.02), "medium"
                        try:
                            response = await asyncio.to_thread(ai_model.generate_content, prompt)
                            raw_json_text = response.text.strip()
                            if "{" in raw_json_text and "}" in raw_json_text:
                                raw_json_text = raw_json_text[raw_json_text.find("{"):raw_json_text.rfind("}")+1]
                            ai_data = json.loads(raw_json_text)
                            lat, lng, severity = float(ai_data.get("lat", lat)), float(ai_data.get("lng", lng)), ai_data.get("severity", severity)
                        except: pass

                        source_display = "France24 Live" if "france24" in feed_url else "RFI Local" if "rfi" in feed_url else "The Local" if "thelocal" in feed_url else "News Desk"
                        
                        incident = {
                            "event": "new_incident",
                            "incident": {
                                "lat": lat, "lng": lng, "category": "LIVE AI INTEL",
                                "description": f"<b>{clean_text[:150]}...</b><br><br>Source: {source_display}",
                                "severity": severity
                            }
                        }
                        cached_incidents.append(incident)
                        if len(cached_incidents) > 30: cached_incidents.pop(0)
                        await manager.broadcast(incident)
                        await asyncio.sleep(4) 
        except Exception as e:
            print(f"Scraper Error for {feed_url}: {e}")

async def background_task():
    await unified_intelligence_scraper()
    while True:
        await asyncio.sleep(60) 
        await unified_intelligence_scraper()

# --- FASTAPI LIFESPAN MANAGER ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Boot up the Telegram native listener
    await telegram_client.start()
    # Boot up the News polling loop
    asyncio.create_task(background_task())
    yield
    # Safely disconnect Telegram when server shuts down
    await telegram_client.disconnect()

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
    return {"status": "Grey Lane OSINT Backend is Live with Native Telegram Integration!"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
