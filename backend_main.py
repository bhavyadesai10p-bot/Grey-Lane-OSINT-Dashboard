import os
import json
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# 1. Initialize the Backend
app = FastAPI(title="Grey Lane OSINT Backend")

# 2. The Bouncer Rule (Fixes the 403 Forbidden error)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 3. Root Endpoint (Fixes the "Not Found" screen on Render)
@app.get("/")
def read_root():
    return {"status": "Grey Lane OSINT Backend is Live and Listening!"}

# 4. WebSocket Endpoint (The pipe to your frontend dark map)
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            # Keeps the pipe open and running
            await websocket.receive_text()
    except WebSocketDisconnect:
        print("Frontend map disconnected")
