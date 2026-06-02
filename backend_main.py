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

# --- THE UNIFIED INTEL PIPELINE ---
FEED_URLS = [
    # Macro Intel (News)
    "https://www.france24.com/en/rss",         
    "https://www.rfi.fr/en/france/rss",        
    "https://www.thelocal.fr/feed",
    # Micro Intel (Telegram via a robust community proxy)
    "https://rsshub.rssforever.com/telegram/channel/BFMTV_news",
    "https://rsshub.rssforever.com/telegram/channel/infotrafic_idf"
]

async def unified_intelligence_scraper():
    global cached_incidents
    
    for feed_url in FEED_URLS:
        try:
            feed = feedparser.parse(feed_url, agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64)')
            
            if feed.entries:
                for entry in reversed(feed.entries[:2]): 
                    raw_content = getattr(entry, 'title', '') + " " + getattr(entry, 'description', '')
                    clean_text = re.sub(r'<[^>]+>', ' ', raw_content).strip()
                    
                    already_exists = any(inc["incident"]["description"].find(clean_text[:20]) != -1 for inc in cached_incidents)
                    
                    if not already_exists and len(clean_text) > 10:
                        prompt = f"""
                        Read this raw intelligence: "{clean_text}"
                        Identify the specific city, street, or landmark mentioned (assume Paris/France if vague).
                        Give the exact latitude and longitude for the location mentioned.
                        If no location is found, use central Paris (48.8566, 2.3522).
                        Respond ONLY with a valid JSON object: {{"lat": 48.8566, "lng": 2.3522, "severity": "high"}}
                        Determine severity (low, medium, high) based on if it mentions protests, police, accidents, etc.
                        """
                        
                        lat = 48.8566 + random.uniform(-0.02, 0.02)
                        lng = 2.3522 + random.uniform(-0.02, 0.02)
                        severity = "medium"
                        
                        try:
                            response = await asyncio.to_thread(ai_model.generate_content, prompt)
                            raw_json_text = response.text.strip()
                            
                            if "{" in raw_json_text and "}" in raw_json_text:
                                raw_json_text = raw_json_text[raw_json_text.find("{"):raw_json_text.rfind("}")+1]
                                
                            ai_data = json.loads(raw_json_text.strip())
                            
                            lat = float(ai_data.get("lat", lat)) + random.uniform(-0.005, 0.005)
                            lng = float(ai_data.get("lng", lng)) + random.uniform(-0.005, 0.005)
                            severity = ai_data.get("severity", severity)
                        except Exception as ai_error:
                            print(f"AI Parse failed for {feed_url}: {ai_error}")

                        is_telegram = "telegram" in feed_url
                        category_name = "TELEGRAM INTEL" if is_telegram else "LIVE AI INTEL"
                        
                        # ---> THE FIX: Restored specific source labels <---
                        if "france24" in feed_url:
                            source_display = "France24 Live"
                        elif "rfi" in feed_url:
                            source_display = "RFI Local"
                        elif "thelocal" in feed_url:
                            source_display = "The Local"
                        elif is_telegram:
                            source_display = feed_url.split('/')[-1]
                        else:
                            source_display = "News Desk"

                        incident = {
                            "event": "new_incident",
                            "incident": {
                                "lat": lat,
                                "lng": lng,
                                "category": category_name,
                                "description": f"<b>{clean_text[:150]}...</b><br><br>Source: {source_display}",
                                "severity": severity
                            }
                        }
                        
                        cached_incidents.append(incident)
                        if len(cached_incidents) > 30: 
                            cached_incidents.pop(0)
                            
                        await manager.broadcast(incident)
                        await asyncio.sleep(4) 
        except Exception as e:
            print(f"Scraper Error for {feed_url}: {e}")

async def background_task():
    await unified_intelligence_scraper()
    while True:
        await asyncio.sleep(60) 
        await unified_intelligence_scraper()

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(background_task())
    yield

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
    return {"status": "Grey Lane OSINT Backend is Live!"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
