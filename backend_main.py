import os
import json
import asyncio
import random
import feedparser
import urllib.request
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

# --- MACRO INTEL PIPES (NEWS) ---
FEED_URLS = [
    "https://www.france24.com/en/rss",         
    "https://www.rfi.fr/en/france/rss",        
    "https://www.thelocal.fr/feed"             
]

# --- MICRO INTEL PIPES (TELEGRAM) ---
# We use the public preview pages (t.me/s/...) to completely avoid API keys and rate limits!
TELEGRAM_CHANNELS = [
    "BFMTV_news",       # General French alerts
    "infotrafic_idf"    # Example transit/traffic alerts for Paris
]

async def scrape_telegram_public():
    global cached_incidents
    for channel in TELEGRAM_CHANNELS:
        url = f"https://t.me/s/{channel}"
        try:
            # We use a standard browser user-agent so Telegram doesn't block the request
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            html = await asyncio.to_thread(urllib.request.urlopen, req)
            html_content = html.read().decode('utf-8')
            
            # Use basic regex to find message text blocks in the static HTML
            messages = re.findall(r'<div class="tgme_widget_message_text[^>]*>(.*?)</div>', html_content, re.DOTALL)
            
            # Grab the 2 most recent messages from the channel
            if messages:
                for msg in reversed(messages[-2:]):
                    # Clean up the raw HTML tags inside the message
                    clean_msg = re.sub(r'<[^>]+>', ' ', msg).strip()
                    
                    # Prevent duplicates
                    already_exists = any(inc["incident"]["description"].find(clean_msg[:20]) != -1 for inc in cached_incidents)
                    
                    if not already_exists and len(clean_msg) > 10:
                        prompt = f"""
                        Read this raw Telegram intel: "{clean_msg}"
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
                            raw_text = response.text.strip()
                            
                            if "{" in raw_text and "}" in raw_text:
                                raw_text = raw_text[raw_text.find("{"):raw_text.rfind("}")+1]
                                
                            ai_data = json.loads(raw_text.strip())
                            
                            lat = float(ai_data.get("lat", lat)) + random.uniform(-0.005, 0.005)
                            lng = float(ai_data.get("lng", lng)) + random.uniform(-0.005, 0.005)
                            severity = ai_data.get("severity", severity)
                        except Exception as ai_error:
                            print(f"Telegram AI Parse failed: {ai_error}")

                        incident = {
                            "event": "new_incident",
                            "incident": {
                                "lat": lat,
                                "lng": lng,
                                "category": "TELEGRAM INTEL",
                                "description": f"<b>Ground Alert:</b><br>{clean_msg[:150]}...<br><br>Source: t.me/{channel}",
                                "severity": severity
                            }
                        }
                        
                        cached_incidents.append(incident)
                        if len(cached_incidents) > 30: 
                            cached_incidents.pop(0)
                            
                        await manager.broadcast(incident)
                        
                        # Speed limit bumper for Telegram processing
                        await asyncio.sleep(4) 
                        
        except Exception as e:
            print(f"Telegram Scraper Error for {channel}: {e}")

async def fetch_and_parse_news():
    global cached_incidents
    for feed_url in FEED_URLS:
        try:
            feed = feedparser.parse(feed_url)
            if feed.entries:
                for entry in reversed(feed.entries[:2]): 
                    already_exists = any(inc["incident"]["description"].find(entry.title) != -1 for inc in cached_incidents)
                    
                    if not already_exists:
                        prompt = f"""
                        Read this news headline: "{entry.title}"
                        Identify the specific city, country, or landmark mentioned. 
                        Give the exact latitude and longitude for the location mentioned.
                        If it is general French/Paris news, use central Paris (48.8566, 2.3522).
                        Respond ONLY with a valid JSON object in this format: {{"lat": 48.8566, "lng": 2.3522, "severity": "medium"}}
                        Determine severity (low, medium, high) based on the headline's tone.
                        """
                        
                        lat = 48.8566 + random.uniform(-0.02, 0.02)
                        lng = 2.3522 + random.uniform(-0.02, 0.02)
                        severity = "medium"
                        
                        try:
                            response = await asyncio.to_thread(ai_model.generate_content, prompt)
                            raw_text = response.text.strip()
                            
                            if "{" in raw_text and "}" in raw_text:
                                raw_text = raw_text[raw_text.find("{"):raw_text.rfind("}")+1]
                                
                            ai_data = json.loads(raw_text.strip())
                            
                            lat = float(ai_data.get("lat", lat)) + random.uniform(-0.005, 0.005)
                            lng = float(ai_data.get("lng", lng)) + random.uniform(-0.005, 0.005)
                            severity = ai_data.get("severity", severity)
                        except Exception as ai_error:
                            print(f"AI Parsing failed for {feed_url}: {ai_error}")

                        source_name = "RFI Local" if "rfi" in feed_url else "The Local" if "thelocal" in feed_url else "France24 Live"

                        incident = {
                            "event": "new_incident",
                            "incident": {
                                "lat": lat,
                                "lng": lng,
                                "category": "LIVE AI INTEL",
                                "description": f"<b>{entry.title}</b><br><br>Source: {source_name}",
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

async def rss_scraper_task():
    # Initial startup sweep
    await fetch_and_parse_news()
    await scrape_telegram_public()
    
    while True:
        # Paced master timer
        await asyncio.sleep(60) 
        await fetch_and_parse_news()
        await scrape_telegram_public()

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
