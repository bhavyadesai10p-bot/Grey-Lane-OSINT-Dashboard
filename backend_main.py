import os
import json
import asyncio
import random
import feedparser
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# --- 1. Connection Manager ---
# This keeps track of every map that is currently open so it can broadcast to all of them
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

manager = ConnectionManager()

# --- 2. The Background RSS Scraper ---
async def rss_scraper_task():
    # We are tapping into the live France24 English news feed
    feed_url = "https://www.france24.com/en/france/rss"
    last_title = ""
    
    while True:
        try:
            # Fetch the live feed
            feed = feedparser.parse(feed_url)
            if feed.entries:
                latest_entry = feed.entries[0]
                
                # Only send a pin if it's a brand new headline we haven't seen yet
                if latest_entry.title != last_title:
                    last_title = latest_entry.title
                    
                    # Generate a randomized coordinate near central Paris for the dashboard
                    lat = 48.8566 + random.uniform(-0.03, 0.03)
                    lng = 2.3522 + random.uniform(-0.04, 0.04)

                    incident = {
                        "event": "new_incident",
                        "incident": {
                            "lat": lat,
                            "lng": lng,
                            "category": "LIVE NEWS (RSS)",
                            "description": f"<b>{latest_entry.title}</b><br><br>Source: France24",
                            "severity": "medium"
                        }
                    }
                    
                    # Broadcast the real headline to the map
                    await manager.broadcast(incident)
                    print(f"Scraped new intel: {latest_entry.title}")
                    
        except Exception as e:
            print(f"Scraper Error: {e}")
        
        # Wait 15 seconds before checking for new intelligence again
        await asyncio.sleep(15)

# --- 3. Initialize App & Start Background Worker ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # This turns on the scraper the second Render boots up
    asyncio.create_task(rss_scraper_task())
    yield

app = FastAPI(title="Grey Lane OSINT Backend", lifespan=lifespan)

# The Bouncer Rule
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
