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
PARIS_CENTER_LAT, PARIS_CENTER_LNG = 48.8566, 2.3522

api_id = int(os.environ.get("TELEGRAM_API_ID", 0))
api_hash = os.environ.get("TELEGRAM_API_HASH", "")
session_string = os.environ.get("TELEGRAM_SESSION", "")
telegram_client = None

# --- SYSTEM 2: INSTANT NEWS SCRAPER (RESTORED PROGRESS) ---
FEED_URLS = ["https://www.france24.com/en/rss", "https://www.rfi.fr/en/france/rss"]

async def unified_intelligence_scraper():
    global cached_incidents
    print("📰 Starting Background Sweep of France24 & RFI...")
    for feed_url in FEED_URLS:
        try:
            feed = feedparser.parse(feed_url)
            if feed.entries:
                for entry in reversed(feed.entries[:5]):
                    raw_content = getattr(entry, 'title', '') + " " + getattr(entry, 'description', '')
                    clean_text = re.sub(r'<[^>]+>', ' ', raw_content).strip()
                    already_exists = any(inc["incident"]["description"].find(clean_text[:20]) != -1 for inc in cached_incidents)
                    
                    if not already_exists and len(clean_text) > 10:
                        # BYPASSING AI FILTER TEMPORARILY TO GET DOTS ON YOUR MAP FAST
                        print(f"📍 Instantly dropping news dot: {clean_text[:40]}...")
                        
                        lat = PARIS_CENTER_LAT + random.uniform(-0.02, 0.02)
                        lng = PARIS_CENTER_LNG + random.uniform(-0.02, 0.02)
                        source_name = "France24" if "france24" in feed_url else "RFI"
                        
                        incident = {
                            "event": "new_incident",
                            "incident": {
                                "lat": lat, "lng": lng, "category": "NEWS",
                                "description": f"<b>{clean_text[:160]}...</b><br><br>Source: {source_name}",
                                "severity": "medium"
                            }
                        }
                        cached_incidents.append(incident)
                        if len(cached_incidents) > 50: cached_incidents.pop(0) 
                        await manager.broadcast(incident)
                        await asyncio.sleep(1) 
        except Exception as e:
            pass
    print("✅ Background Sweep Complete.")

async def background_task():
    # Push immediate test ping to prove websocket works
    await manager.broadcast({"event": "new_incident", "incident": {"lat": PARIS_CENTER_LAT, "lng": PARIS_CENTER_LNG, "category": "SYSTEM", "description": "SERVER ONLINE", "severity": "low"}})
    
    await unified_intelligence_scraper()
    while True:
        await asyncio.sleep(60) 
        await unified_intelligence_scraper()

# --- FASTAPI LIFESPAN ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global telegram_client
    print("🚀 Booting OSINT Server...")
    
    # 1. SAFELY INITIALIZE TELEGRAM (NON-BLOCKING)
    telegram_client = TelegramClient(StringSession(session_string), api_id, api_hash)
    
    @telegram_client.on(events.NewMessage())
    async def telegram_handler(event):
        print(f"🚨 RAW TELEGRAM PING: {event.raw_text[:60]}")

    try:
        # Connect safely instead of 'start' so it doesn't freeze asking for a password
        await telegram_client.connect()
        if not await telegram_client.is_user_authorized():
            print("❌ TELEGRAM SESSION IS DEAD. You need to generate a new string locally.")
        else:
            print("🟢 Telegram is authorized and listening!")
    except Exception as e:
        print(f"⚠️ Telegram Connection Error: {e}")
        
    # 2. START NEWS SCRAPER INDEPENDENTLY
    asyncio.create_task(background_task())
    
    yield
    try: await telegram_client.disconnect()
    except: pass

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
