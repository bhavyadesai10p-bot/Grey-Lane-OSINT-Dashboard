# Paris Urban Safety & Transit OSINT Map

Real-time OSINT dashboard for Paris — aggregates live incident data and
public transit updates onto a unified interactive map for tourists and residents.

---

## Project Structure

```
paris-osint-map/
├── frontend/
│   └── paris-osint-map.html     # ← Single-file production frontend (open this!)
│
├── backend/
│   ├── main.py                  # FastAPI app, all routes, WebSocket
│   ├── models.py                # SQLAlchemy ORM models
│   ├── pipeline/
│   │   ├── ratp.py              # IDFM PRIM API ingestion
│   │   ├── rss.py               # RSS news feed parser
│   │   ├── social.py            # X/Twitter + Telegram scraper
│   │   └── ai_parser.py        # Anthropic Claude middleware
│   ├── services/
│   │   ├── geocode.py           # Nominatim geocoding
│   │   ├── bundler.py           # Multi-source incident bundling
│   │   └── route_check.py       # Route safety intersection engine
│   └── schema.sql               # PostgreSQL schema
│
├── .env.example
├── requirements.txt
└── README.md
```

---

## Quick Start — Frontend Only

The frontend (`paris-osint-map.html`) runs **standalone** in any modern browser.
No server required. Just open it:

```bash
open paris-osint-map.html
# or: python3 -m http.server 8080 then open http://localhost:8080
```

To enable the **AI Incident Parser**, add your Anthropic API key to the
`parseWithAI()` function in the HTML file, or proxy through your backend.

---

## Backend Setup

### 1. Prerequisites
- Python 3.11+
- PostgreSQL 15+ (with PostGIS optional but recommended)
- API keys: see `.env.example`

### 2. Install dependencies
```bash
python -m venv .venv && source .venv/bin/activate
pip install fastapi uvicorn sqlalchemy asyncpg aiohttp feedparser anthropic python-dotenv
```

### 3. Configure environment
```bash
cp .env.example .env
# Edit .env with your credentials
```

### 4. Create database
```bash
createdb paris_osint
psql paris_osint < schema.sql
```

### 5. Run the server
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

API docs: http://localhost:8000/docs

---

## Environment Variables

```env
# Database
DATABASE_URL=postgresql+asyncpg://user:password@localhost/paris_osint

# Anthropic (for AI parsing middleware)
ANTHROPIC_API_KEY=sk-ant-...

# IDFM PRIM API (official Paris transit)
# Register free at: https://prim.iledefrance-mobilites.fr/
IDFM_API_KEY=your_key_here

# Scrapfly (social media scraping proxy)
# Register at: https://scrapfly.io/
SCRAPFLY_API_KEY=scp-live-...
```

---

## API Reference

| Method | Endpoint             | Description                            |
|--------|----------------------|----------------------------------------|
| GET    | /incidents           | List active incidents (filterable)     |
| POST   | /incidents           | Manually submit an incident            |
| DELETE | /incidents/{id}      | Mark incident resolved                 |
| GET    | /incidents/bundles   | List multi-source bundles              |
| GET    | /strikes             | Upcoming/active strike events          |
| GET    | /transit/alerts      | Live RATP/IDFM disruptions             |
| GET    | /transit/lines       | Static metro/RER GeoJSON               |
| POST   | /parse               | AI-parse raw text → incident           |
| GET    | /geocode?q=...       | Geocode a Paris address                |
| POST   | /route/check         | Check route for hazards                |
| GET    | /heatmap             | Temporal density data                  |
| GET    | /stats               | Dashboard summary stats                |
| WS     | /ws/live             | WebSocket live push feed               |

---

## Incident Schema

Every incident object:

```json
{
  "id":            1,
  "uuid":          "3f1a2b...",
  "lat":           48.8566,
  "lng":           2.3522,
  "address":       "Bd Saint-Germain, Paris 6e",
  "arrondissement": 6,
  "category":      "A",
  "type":          "Spontaneous Protest",
  "severity":      4,
  "description":   "Wildcat manif blocking Bd St-Germain. CRS deployed.",
  "source_url":    "https://twitter.com/...",
  "source_type":   "social",
  "verified":      false,
  "media_url":     null,
  "occurred_at":   "2025-06-01T16:45:00Z",
  "bundled":       false
}
```

### Category Taxonomy

| Code | Category                        | Severity Range |
|------|---------------------------------|----------------|
| A    | Civil Unrest & Mass Mobilisation | 1–5            |
| B    | Property Damage & Destruction   | 3–5            |
| C    | Street Crime & Personal Safety  | 1–4            |
| D    | Transit & Infrastructure        | 2–3            |
| E    | High-Level Security Alerts      | 4–5            |

---

## Frontend Features

| Feature                    | Description                                              |
|----------------------------|----------------------------------------------------------|
| Interactive map            | Leaflet.js, dark tile layer, Paris-centered              |
| Category toggles           | Show/hide each of the 5 incident categories              |
| Severity filter            | Slider to filter min severity 1–5                        |
| Temporal heat map slider   | Filter incidents by hour of day                          |
| Verified / Unverified      | Toggle verified (solid ring) vs unverified (dashed ring) |
| Transit overlay            | Toggle Métro lines + RER lines with station markers      |
| Strike calendar            | Fly to pre-announced strike polygon zones                |
| Route Safety Guard         | A→B path cross-referenced against Sev 3+ incidents      |
| AI Incident Parser         | Paste raw text → Claude → structured incident on map     |
| Live feed panel            | Sortable incident list; click to fly to location         |
| Mobile bottom sheet        | Swipeable drawer on mobile (Google Maps style)           |
| Multi-source bundling      | Social + official pins within 200m auto-merged           |
| Live clock                 | Paris CET time in header                                 |
| Orientation-safe           | Map center preserved on device rotation                  |

---

## Data Sources

### Official (Verified)
- **IDFM PRIM API** — Île-de-France Mobilités real-time traffic messages
- **Prefecture de Police Paris** — authorised protest notifications
- **Service-Public.fr** — government alerts

### Semi-Verified (News)
- Le Monde RSS
- Le Figaro RSS
- BFMTV Paris RSS
- 20 Minutes Paris RSS

### Unverified (Social / OSINT)
- X/Twitter keyword monitoring (via Scrapfly proxy)
- Telegram public OSINT channels
- User submissions via the frontend parser

---

## Multi-Source Bundling Logic

When a new incident is ingested, the bundler:
1. Queries all active incidents within **200 metres** radius
2. Within a **1-hour** time window
3. Matching the **same category**
4. Where **at least one source is verified**

If a match is found, both incidents are merged into a **bundle**:
- The unverified incident inherits `verified: true`
- The map renders a green glowing ring around the merged pin
- The popup shows both source URLs

---

## Deployment

### Docker (recommended)

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

```bash
docker build -t paris-osint-api .
docker run -p 8000:8000 --env-file .env paris-osint-api
```

### Frontend hosting
Deploy `paris-osint-map.html` to any static host (Netlify, GitHub Pages, Vercel).
Set the API base URL in the frontend constants.

---

## License
MIT — built for public safety and urban intelligence research.
