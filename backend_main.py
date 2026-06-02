"""
Paris Urban Safety & Transit OSINT Map — FastAPI Backend
=========================================================
Full production backend with:
  - PostgreSQL schema (via SQLAlchemy ORM)
  - Multi-source ingestion pipeline (RATP API, RSS feeds, Social scraper)
  - LLM parsing middleware (Google Gemini)
  - Geocoding via Nominatim
  - Multi-source incident bundling
  - REST API for frontend
  - WebSocket live feed
"""

# ── Dependencies ──────────────────────────────────────────────────────────────
# pip install fastapi uvicorn sqlalchemy asyncpg aiohttp feedparser google-generativeai python-dotenv

from __future__ import annotations
import os, asyncio, json, math, hashlib, text
