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

# --- SYSTEM 1: TACTICAL NEWS SCRAPER WITH AI GEOCODING ---
FEED_URLS = ["https://www.france24.com/en/rss", "https://www.rfi.fr/en/france/rss"]

async def unified_intelligence_scraper():
    global cached_incidents
    print("📰 Sweeping France24 & RFI with AI Geocoding Engine...")
    for feed_url in FEED_URLS:
        try:
            feed = feedparser.parse(feed_url)
            if feed.entries:
                for entry in reversed(feed.entries[:5]):
                    title = getattr(entry, 'title', '')
                    description = getattr(entry, 'description', '')
                    raw_content = title + " " + description
                    clean_text = re.sub(r'<[^>]+>', ' ', raw_content).strip()
                    
                    # Prevent duplication
                    already_exists = any(inc["incident"]["description"].find(clean_text[:20]) != -1 for inc in cached_incidents)
                    
                    if not already_exists and len(clean_text) > 10:
                        source_name = "France24" if "france24" in feed_url else "RFI"
                        print(f"🧠 Processing report with Gemini: {title[:50]}...")

                        prompt = f"""
                        Analyze this news report headline/body for urban threat mapping:
                        Title: "{title}"
                        Context: "{description}"

                        CRITICAL RULES:
                        1. Determine if this event is related to security, civil unrest, crime, protests, transit, or infrastructure disruptions happening in Paris or Île-de-France. If it is a generic nationwide policy news, international sports news, or completely unrelated to urban threat/transit safety, set "relevant_to_paris_safety" to false.
                        2. If a specific Paris street, metro station, neighborhood, or landmark is mentioned, provide its precise latitude and longitude.
                        3. If it is relevant to Paris safety but no specific street/landmark is mentioned, set "exact_location_found" to false and use default center coordinates (48.8566, 2.3522).
                        4. Map "category" strictly to one of these: PROTEST, DAMAGE, CRIME, TRANSIT, or SECURITY.
                        5. Determine "severity" strictly as: low, medium, or high.

                        Respond ONLY with this valid JSON template. No markdown formatting, no comments, no extra text:
                        {{"relevant_to_paris_safety": true, "exact_location_found": true, "lat": 48.8566, "lng": 2.3522, "severity": "medium", "category": "PROTEST"}}
                        """

                        try:
                            response = await asyncio.to_thread(ai_model.generate_content, prompt)
                            raw_json = response.text.strip()
                            
                            # Clean up potential markdown wrappers from AI output
                            if "{" in raw_json: 
                                raw_json = raw_json[raw_json.find("{"):raw_json.rfind("}")+1]
                            
                            ai_data = json.loads(raw_json)
                            
                            # Filter out non-tactical noise or unrelated national news
                            if not ai_data.get("relevant_to_paris_safety", False):
                                continue
                                
                            lat = float(ai_data.get("lat", PARIS_CENTER_LAT))
                            lng = float(ai_data.get("lng", PARIS_CENTER_LNG))
                            
                            # Add a tiny micro-random fuzz ONLY if it mapped to the dead center default 
                            # to prevent dots perfectly stacking on top of each other.
                            if not ai_data.get("exact_location_found", False):
                                lat += random.uniform(-0.005, 0.005)
                                lng += random.uniform(-0.005, 0.005)

                            incident = {
                                "event": "new_incident",
                                "incident": {
                                    "lat": lat, 
                                    "lng": lng, 
                                    "category": ai_data.get("category", "NEWS").upper(),
                                    "description": f"<b>{title}</b><br><br>{description[:140]}...<br><br>Source: {source_name}",
                                    "severity": ai_data.get("severity", "medium").lower()
                                }
                            }
                            
                            cached_incidents.append(incident)
                            if len(cached_incidents) > 50: 
                                cached_incidents.pop(0) 
                                
                            await manager.broadcast(incident)
                            print(f"📍 AI MAP DROP: [{ai_data.get('category')}] at ({lat:.4f}, {lng:.4f})")
                            await asyncio.sleep(1)
                            
                        except Exception as ai_err:
                            print(f"⚠️ Geocoding AI Extraction skipped: {ai_err}")
                            
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
    print("🚀 Booting AI-Powered OSINT Grey Lane Server...")
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
