# RentScout

**Find rentals ranked by your commute.** RentScout aggregates listings from NoBroker and Telegram groups, deduplicates them, and shows everything on a single map — sorted by distance from your workplace.

**Live:** [rental-aggregator.onrender.com](https://rental-aggregator.onrender.com)

## What it does

- Searches **NoBroker** (direct API) and **Telegram** rental channels for Bangalore
- Deduplicates listings across sources
- Shows all listings on a **Leaflet map** with price markers and clustering
- Ranks by **distance from your office/home** when you enter an address
- Listing detail panel with **photos**, BHK, furnishing, price, and direct link to source
- **Copy Inquiry** generates a ready-to-send message for the landlord (via Claude API)
- Auto-refreshes listings every 24 hours

## Tech stack

| Layer | Tech |
|---|---|
| Backend | Python, FastAPI, uvicorn |
| Frontend | Vanilla HTML/CSS/JS, Leaflet.js, Leaflet.markercluster |
| Database | Turso (cloud SQLite via libsql) |
| Scrapers | httpx (NoBroker API), BeautifulSoup (Telegram public channels) |
| AI | Anthropic Claude API (Telegram post extraction, inquiry message generation) |
| Geocoding | Google Maps Geocoding API |
| Hosting | Render (web service) |

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────┐
│  NoBroker   │────▶│              │     │  Turso  │
│  API v3     │     │  Orchestrator│────▶│ (Cloud  │
│             │     │              │     │ SQLite) │
├─────────────┤     │  - Scrape    │     └────┬────┘
│  Telegram   │────▶│  - Extract   │          │
│  t.me/s/    │     │  - Dedup     │          ▼
└─────────────┘     │  - Geocode   │     ┌─────────┐
                    └──────────────┘     │ FastAPI │
                                         │ Server  │
                                         └────┬────┘
                                              │
                                              ▼
                                         ┌─────────┐
                                         │ Browser │
                                         │ Leaflet │
                                         │ Map     │
                                         └─────────┘
```

## Data sources

| Source | Method | Data quality |
|---|---|---|
| **NoBroker** | Direct API (`/api/v3/multi/property/RENT/filter`) with Google Places `placeId` | High — structured price, BHK, location, photos, coordinates |
| **Telegram** | Public channel preview scraping (`t.me/s/CHANNEL`) | Medium — unstructured text, Claude extracts price/BHK/area |

Telegram channels scraped: `@HousingBangalore`, `@housingourbengaluru`

## Local development

```bash
# Clone
git clone https://github.com/vishvesh245/rental-aggregator.git
cd rental-aggregator

# Install deps
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Set env vars
cp .env.example .env
# Edit .env with your keys:
#   ANTHROPIC_API_KEY=sk-ant-...
#   GOOGLE_MAPS_API_KEY=AIza...
#   TURSO_DB_URL=libsql://your-db.turso.io
#   TURSO_AUTH_TOKEN=eyJ...

# Run
uvicorn server:app --host 0.0.0.0 --port 8000
# Open http://localhost:8000
```

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `TURSO_DB_URL` | Yes | Turso database URL (`libsql://...`) |
| `TURSO_AUTH_TOKEN` | Yes | Turso auth token |
| `ANTHROPIC_API_KEY` | Yes | For Claude extraction + inquiry messages |
| `GOOGLE_MAPS_API_KEY` | Yes | For geocoding addresses and resolving NoBroker placeIds |
| `APIFY_API_TOKEN` | No | Legacy — was used for 99acres/Housing.com (deprecated) |

## Deployment (Render)

The app deploys to Render via `render.yaml`. On push to `main`, Render auto-builds and deploys.

```yaml
# render.yaml
services:
  - type: web
    name: rentscout
    runtime: python
    buildCommand: pip install -r requirements.txt
    startCommand: uvicorn server:app --host 0.0.0.0 --port $PORT
```

Add env vars in Render dashboard → Environment tab.

## Project structure

```
├── server.py              # FastAPI app — API routes + static file serving
├── config.py              # Environment variable loading
├── models.py              # RawPost, RentalListing dataclasses
├── pipeline/
│   ├── orchestrator.py    # Main pipeline — scrape, extract, dedup, geocode, store
│   ├── scraper_nobroker.py # NoBroker API scraper
│   ├── scraper_telegram.py # Telegram public channel scraper
│   ├── extractor.py       # Claude-based extraction for unstructured posts
│   ├── geocoder.py        # Google Maps geocoding
│   ├── dedup.py           # Cross-source deduplication
│   ├── storage.py         # Turso/SQLite storage layer
│   └── base_scraper.py    # Base scraper class + SearchParams
├── static/
│   └── index.html         # Single-page app (landing + dashboard)
├── requirements.txt
└── render.yaml
```

## License

MIT
