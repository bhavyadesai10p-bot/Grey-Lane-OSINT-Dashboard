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

# --- SYSTEM 1: TACTICAL NEWS SCRAPER ---
FEED_URLS = ["https://www.france24.com/en/rss", "https://www.rfi.fr/en/france/rss"]

async def unified_intelligence_scraper():
    global cached_incidents
    print("📰 Sweeping France24 & RFI...")
    for feed_url in FEED_URLS:
        try:
            feed = feedparser.parse(feed_url)
            if feed.entries:
                for entry in reversed(feed.entries[:5]):
                    raw_content = getattr(entry, 'title', '') + " " + getattr(entry, 'description', '')
                    clean_text = re.sub(r'<[^>]+>', ' ', raw_content).strip()
                    already_exists = any(inc["incident"]["description"].find(clean_text[:20]) != -1 for inc in cached_incidents)
                    
                    if not already_exists and len(clean_text) > 10:
                        # Temporary randomized Paris offset until we reactivate geo-filtering next
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
    await unified_intelligence_scraper()
    while True:
        await asyncio.sleep(60) 
        await unified_intelligence_scraper()

# --- FASTAPI LIFESPAN ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Booting Pure-Tactical Grey Lane Server...")
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
