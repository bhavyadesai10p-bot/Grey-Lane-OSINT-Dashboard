import os
import json
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# 1. Initialize the Backend
app = FastAPI(title="Grey Lane OSINT Backend")

# 2. The Bouncer Rule
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 3. Root Endpoint
@app.get("/")
def read_root():
    return {"status": "Grey Lane OSINT Backend is Live and Listening!"}

# 4. WebSocket Endpoint (Now with a simulated Intel Payload)
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    
    # --- THE PAYLOAD TEST ---
    # Wait 3 seconds so you have time to see it happen on the map
    await asyncio.sleep(3)
    
    # Construct the intelligence packet
    test_incident = {
        "event": "new_incident",
        "incident": {
            "lat": 48.8606, 
            "lng": 2.3376, # Coordinates for the Louvre
            "category": "security alert",
            "description": "Unplanned political demonstration blocking main thoroughfare. Security perimeter established.",
            "severity": "high"
        }
    }
    
    try:
        # Fire the payload down the pipe to the frontend
        await websocket.send_json(test_incident)
        
        # Keep the pipe open
        while True:
            await websocket.receive_text()
            
    except WebSocketDisconnect:
        print("Frontend map disconnected")
