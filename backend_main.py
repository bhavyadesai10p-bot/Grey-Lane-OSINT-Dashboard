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

# --- SYSTEM 1: TELEGRAM FLOODGATE LISTENER ---
api_id = int(os.environ.get("TELEGRAM_API_ID", 0))
api_hash = os.environ.get("TELEGRAM_API_HASH", "")
session_string = os.environ.get("TELEGRAM_SESSION", "")

telegram_client = TelegramClient(StringSession(session_string), api_id, api_hash)

# We removed the strict chats= filter to bypass the Telethon bug. 
# It will now catch the message first, THEN verify the channel name.
@telegram_client.on(events.NewMessage())
async def telegram_handler(event):
    global cached_incidents
    raw_text = event.raw_text
    
    # Get the name of the chat safely
    try:
        chat = await event.get_chat()
        chat_name = getattr(chat, 'username', None) or getattr(chat, 'title', 'Unknown')
    except:
        chat_name = "Unknown"
        
    print(f"🚨 RAW TELEGRAM PING [{chat_name}]: {str(raw_text)[:60]}...")
    
    # Manually filter for our targets
    allowed_channels = ["BFMTV_news", "infotrafic_idf", "greylane_test_paris"]
    if chat_name not in allowed_channels:
        return 
        
    print(f"✅ TARGET MATCHED! Geocoding message from {chat_name}...")

    prompt = f"""
    Analyze this tactical intelligence report: "{raw_text}"
    CRITICAL RULES:
    1. Determine if this event is happening in Paris or Île-de-France. If NOT, set "is_paris" to false.
    2. If a specific street/landmark is found, provide its exact lat/lng.
    3. If general Paris alert, set "exact_location_found" to false and use default (48.8566, 2.3522).
    Respond ONLY with this valid JSON:
    {{"is_paris": true, "exact_location_found": true, "lat": 48.8566, "lng": 2.3522, "severity": "high", "category": "PROTEST"}}
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

        incident = {
            "event": "new_incident",
            "incident": {
                "lat": lat, "lng": lng, 
                "category": ai_data.get("category", "TELEGRAM").upper(),
                "description": f"<b>🚨 LIVE PARIS INTEL</b><br>{raw_text[:160]}...<br><br>Source: t.me/{chat_name}",
                "severity": ai_data.get("severity", "medium")
            }
        }
        cached_incidents.append(incident)
        if len(cached_incidents) > 200: cached_incidents.pop(0) 
        await manager.broadcast(incident)
        print("📍 TELEGRAM DOT DROPPED ON MAP!")
    except Exception as e:
        print(f"❌ Telegram AI Error: {e}")

# --- SYSTEM 2: LOUD NEWS SCRAPER ---
FEED_URLS = ["https://www.france24.com/en/rss", "https://www.rfi.fr/en/france/rss"]

async def unified_intelligence_scraper():
    global cached_incidents
    print("📰 Starting Background Sweep of France24 & RFI...")
    for feed_url in FEED_URLS:
        try:
            feed = feedparser.parse(feed_url)
            if feed.entries:
                for entry in reversed(feed.entries[:5]): # Reduced to 5 so it processes faster
                    raw_content = getattr(entry, 'title', '') + " " + getattr(entry, 'description', '')
                    clean_text = re.sub(r'<[^>]+>', ' ', raw_content).strip()
                    already_exists = any(inc["incident"]["description"].find(clean_text[:20]) != -1 for inc in cached_incidents)
                    
                    if not already_exists and len(clean_text) > 10:
                        prompt = f"""
                        Analyze this news report: "{clean_text}"
                        If this event is not explicitly happening in Paris/Île-de-France, set "is_paris" to false.
                        If it is in Paris, extract exact lat/lng.
                        Respond ONLY with this JSON layout:
                        {{"is_paris": true, "exact_location_found": true, "lat": 48.8566, "lng": 2.3522, "severity": "medium", "category": "SECURITY"}}
                        """
                        try:
                            response = await asyncio.to_thread(ai_model.generate_content, prompt)
                            raw_json = response.text.strip()
                            if "{" in raw_json: raw_json = raw_json[raw_json.find("{"):raw_json.rfind("}")+1]
                            ai_data = json.loads(raw_json)
                            
                            if not ai_data.get("is_paris", False):
                                continue 
                            
                            print(f"📍 Found Paris News! Geocoding: {clean_text[:40]}...")
                            lat = float(ai_data.get("lat", PARIS_CENTER_LAT))
                            lng = float(ai_data.get("lng", PARIS_CENTER_LNG))
                            
                            incident = {
                                "event": "new_incident",
                                "incident": {
                                    "lat": lat, "lng": lng, "category": ai_data.get("category", "NEWS").upper(),
                                    "description": f"<b>{clean_text[:160]}...</b><br><br>Source: News Desk",
                                    "severity": ai_data.get("severity", "medium")
                                }
                            }
                            cached_incidents.append(incident)
                            if len(cached_incidents) > 200: cached_incidents.pop(0) 
                            await manager.broadcast(incident)
                            await asyncio.sleep(2) 
                        except: 
                            continue
        except Exception as e:
            pass
    print("✅ Background Sweep Complete.")

async def background_task():
    await unified_intelligence_scraper()
    while True:
        await asyncio.sleep(60) 
        await unified_intelligence_scraper()

# --- FASTAPI LIFESPAN ---
async def telegram_connect_task():
    print("🚀 Booting Telegram Matrix...")
    try:
        await telegram_client.start()
        print("🟢 Grey Lane Telethon Wire established successfully!")
    except Exception as e:
        print(f"⚠️ Session error: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(telegram_connect_task())
    asyncio.create_task(background_task())
    yield
    try:
        await telegram_client.disconnect()
    except: pass

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
