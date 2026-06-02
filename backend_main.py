import os
import json
import asyncio
import random
import feedparser
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import google.generativeai as genai

# --- AI SETUP ---
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
ai_model = genai.GenerativeModel('gemini-1.5-flash')

cached_incidents = []

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        # Instantly send whatever is in memory
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

# We broke the scraping logic into its own function so we can force-run it at startup
async def fetch_and_parse_news():
    global cached_incidents
    feed_url = "https://www.france24.com/en/france/rss"
    try:
        feed = feedparser.parse(feed_url)
        if feed.entries:
            for entry in reversed(feed.entries[:5]): 
                already_exists = any(inc["incident"]["description"].find(entry.title) != -1 for inc in cached_incidents)
                
                if not already_exists:
                    prompt = f"""
                    Read this news headline from France: "{entry.title}"
                    Identify the specific city or landmark mentioned. 
                    If it mentions a specific place (like Louvre, Eiffel Tower, Marseille, Saint-Denis), give the exact latitude and longitude for it.
                    If it's a general France headline, give the coordinates for central Paris (48.8566, 2.3522).
                    Respond ONLY with a valid JSON object in this exact format, with no extra text: {{"lat": 48.8566, "lng": 2.3522, "severity": "medium"}}
                    Determine severity (low, medium, high) based on the headline's tone.
                    """
                    
                    lat = 48.8566
                    lng = 2.3522
                    severity = "medium"
                    
                    try:
                        response = ai_model.generate_content(prompt)
                        raw_text = response.text.strip()
                        if raw_text.startswith("```"):
                            raw_text = raw_text.split("\n", 1)[1]
                        if raw_text.endswith("```"):
                            raw_text = raw_text.rsplit("\n", 1)[0]
                            
                        ai_data = json.loads(raw_text.strip())
                        lat = float(ai_data.get("lat", lat))
                        lng = float(ai_data.get("lng", lng))
                        severity = ai_data.get("severity", severity)
                    except Exception as ai_error:
                        print(f"AI Parsing failed, using defaults: {ai_error}")

                    incident = {
                        "event": "new_incident",
                        "incident": {
                            "lat": lat,
                            "lng": lng,
                            "category": "AI PARSED INTELLIGENCE",
                            "description": f"<b>{entry.title}</b><br><br>Source: France24",
                            "severity": severity
                        }
                    }
                    
                    cached_incidents.append(incident)
                    if len(cached_incidents) > 20:
                        cached_incidents.pop(0)
                        
                    await manager.broadcast(incident)
    except Exception as e:
        print(f"Scraper Error: {e}")

async def rss_scraper_task():
    # FORCE FIRST FETCH: Fill memory immediately on boot before starting the timer loop
    await fetch_and_parse_news()
    
    while True:
        # Check for updates every 30 seconds ongoing
        await asyncio.sleep(30)
        await fetch_and_parse_news()

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(rss_scraper_task())
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
    return {"status": "Grey Lane OSINT Backend is Live and Powered by AI!"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
