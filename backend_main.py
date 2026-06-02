import os
import json
import asyncio
import random
import feedparser
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# Global storage to keep track of the latest incidents so new visitors see them instantly
cached_incidents = []

# --- 1. Connection Manager ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        
        # INSTANT SYNC: Send all currently cached headlines to this new user immediately
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

# --- 2. The Background RSS Scraper ---
async def rss_scraper_task():
    global cached_incidents
    feed_url = "https://www.france24.com/en/france/rss"
    
    while True:
        try:
            feed = feedparser.parse(feed_url)
            if feed.entries:
                # Process the top 5 latest articles to populate the map initially
                new_items_found = False
                
                for entry in reversed(feed.entries[:5]): # Look at recent 5 entries
                    # Check if we already have this headline cached
                    already_exists = any(inc["incident"]["description"].find(entry.title) != -1 for inc in cached_incidents)
                    
                    if not already_exists:
                        # Generate a coordinate near central Paris
                        lat = 48.8566 + random.uniform(-0.03, 0.03)
                        lng = 2.3522 + random.uniform(-0.04, 0.04)

                        incident = {
                            "event": "new_incident",
                            "incident": {
                                "lat": lat,
                                "lng": lng,
                                "category": "LIVE NEWS (RSS)",
                                "description": f"<b>{entry.title}</b><br><br>Source: France24",
                                "severity": "medium"
                            }
                        }
                        
                        # Add to our server's memory cache
                        cached_incidents.append(incident)
                        # Keep cache size capped at 20 items so it doesn't clutter
                        if len(cached_incidents) > 20:
                            cached_incidents.pop(0)
                            
                        # Broadcast live to anyone looking at the map right now
                        await manager.broadcast(incident)
                        print(f"Cached & streamed new intel: {entry.title}")
                        
        except Exception as e:
            print(f"Scraper Error: {e}")
        
        # Check for updates every 30 seconds
        await asyncio.sleep(30)

# --- 3. Initialize App ---
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
    return {"status": "Grey Lane OSINT Backend is Live!"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
