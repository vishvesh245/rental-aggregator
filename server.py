"""FastAPI server for the Rental Aggregator dashboard."""

from __future__ import annotations

import asyncio
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import googlemaps
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import config
from pipeline.scraper_facebook import scrape_facebook_group
from pipeline.extractor import extract_rental_details
from pipeline.geocoder import geocode_listings
from pipeline.storage import (
    init_db, get_listings, get_listings_summary, get_stats, save_raw_posts, save_listings,
    save_preferences, get_preferences, delete_preferences, save_email_capture,
)
from pipeline.orchestrator import run_full_pipeline, ALL_SCRAPERS
from pipeline.base_scraper import SearchParams
from pipeline.deduplicator import get_canonical_listings
from pipeline.message_generator import generate_broker_message

app = FastAPI(title="Rental Aggregator")

# Initialize DB on startup
init_db()

# Google Maps client for geocoding
gmaps = googlemaps.Client(key=config.GOOGLE_MAPS_API_KEY) if config.GOOGLE_MAPS_API_KEY else None

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def index():
    return FileResponse("static/index.html")


@app.get("/api/listings/summary")
async def api_listings_summary():
    """Return a summary of cached listings for the landing page hero card."""
    return get_listings_summary()


@app.get("/api/listings")
async def api_listings(
    city: str = "",
    area: str = "",
    min_price: int | None = None,
    max_price: int | None = None,
    bedrooms: int | None = None,
    type: str = "supply",
    listing_type: str = "",
    furnished: str = "",
    source: str = "",
    dedup: bool = False,
    days: int | None = None,
):
    post_type = type if type != "all" else ""
    listings = get_listings(
        city=city,
        area=area,
        price_min=min_price,
        price_max=max_price,
        bedrooms=bedrooms,
        post_type=post_type,
        listing_type=listing_type,
        furnished=furnished,
        source=source,
        days=days,
    )
    if dedup:
        listings = get_canonical_listings(listings)
    return {"listings": listings, "count": len(listings)}


@app.api_route("/api/stats", methods=["GET", "HEAD"])
async def api_stats():
    return get_stats()


@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    return {"status": "ok"}


class EmailCaptureRequest(BaseModel):
    email: str
    city: str = ""
    bedrooms: list[int] | str = ""


@app.post("/api/capture-email")
async def api_capture_email(req: EmailCaptureRequest):
    bedrooms_str = str(req.bedrooms) if req.bedrooms else ""
    saved = save_email_capture(req.email, req.city, bedrooms_str)
    return {"status": "saved" if saved else "exists"}


@app.get("/api/places")
async def api_places(q: str = ""):
    """Return location suggestions using geocoding API."""
    if not q or len(q) < 2:
        return {"suggestions": []}
    if not gmaps:
        return {"suggestions": []}

    try:
        results = gmaps.geocode(q + ", India")
        suggestions = []
        for r in results[:5]:
            addr = r.get("formatted_address", "")
            parts = addr.split(",", 1)
            suggestions.append({
                "place_id": r.get("place_id", ""),
                "main": parts[0].strip(),
                "secondary": parts[1].strip() if len(parts) > 1 else "",
                "description": addr,
            })
        return {"suggestions": suggestions}
    except Exception:
        return {"suggestions": []}


@app.get("/api/geocode")
async def api_geocode(q: str = "", place_id: str = ""):
    """Geocode a location string or place_id to lat/lng coordinates."""
    if not q and not place_id:
        return {"error": "Query parameter 'q' or 'place_id' is required"}
    if not gmaps:
        return {"error": "Google Maps API key not configured"}

    if place_id:
        results = gmaps.geocode(place_id=place_id)
    else:
        results = gmaps.geocode(q + ", India")
    if not results:
        return {"error": f"Could not geocode: {q}"}

    loc = results[0]["geometry"]["location"]
    return {
        "lat": loc["lat"],
        "lng": loc["lng"],
        "formatted": results[0].get("formatted_address", q),
    }


# --- Scrape endpoint ---

_executor = ThreadPoolExecutor(max_workers=2)
_scrape_jobs: dict[str, dict] = {}  # job_id -> {status, result, error}


class ScrapeRequest(BaseModel):
    group_url: str
    period: str = "7d"
    city: str = ""
    max_posts: int = 5
    mode: str = "group"  # "group" = full group import, "post" = single post only


def _run_pipeline(job_id: str, group_url: str, period: str, city: str, max_posts: int):
    """Run the full scrape→extract→geocode→store pipeline (blocking)."""
    try:
        _scrape_jobs[job_id]["status"] = "scraping"
        posts = scrape_facebook_group(group_url, time_period=period, max_posts=max_posts)
        saved_posts = save_raw_posts(posts)

        _scrape_jobs[job_id]["status"] = "extracting"
        listings = extract_rental_details(posts)

        _scrape_jobs[job_id]["status"] = "geocoding"
        listings = geocode_listings(listings, default_city=city)
        saved_listings = save_listings(listings)

        _scrape_jobs[job_id].update({
            "status": "done",
            "_ts": time.time(),
            "result": {
                "posts_scraped": len(posts),
                "listings_extracted": len(listings),
                "listings_saved": saved_listings,
            },
        })
    except Exception as e:
        _scrape_jobs[job_id].update({"status": "error", "_ts": time.time(), "error": str(e)})


_FB_GROUP_RE = re.compile(
    r"^https?://(www\.|m\.)?facebook\.com/groups/[A-Za-z0-9._-]+/?$",
)
_FB_POST_RE = re.compile(
    r"^https?://(www\.|m\.)?facebook\.com/groups/(?P<group>[A-Za-z0-9._-]+)/(posts|permalink)/\d+",
)
_JOB_TTL = 3600  # 1 hour


def _cleanup_old_jobs():
    """Remove completed/errored jobs older than TTL."""
    now = time.time()
    expired = [
        jid for jid, job in _scrape_jobs.items()
        if job.get("_ts") and now - job["_ts"] > _JOB_TTL
        and job["status"] in ("done", "error")
    ]
    for jid in expired:
        del _scrape_jobs[jid]


@app.post("/api/scrape")
async def api_scrape(req: ScrapeRequest):
    """Start an import job. Returns a job ID to poll for status."""
    # Determine the group URL to import from
    group_url = req.group_url.strip()
    max_posts = req.max_posts

    post_match = _FB_POST_RE.match(group_url)
    group_match = _FB_GROUP_RE.match(group_url)

    if post_match:
        # Post URL detected — extract group and decide behavior
        group_slug = post_match.group("group")
        if req.mode == "post":
            # Single post import: pass the post URL directly, limit to 1
            max_posts = 1
        else:
            # Full group import: construct group URL from the post URL
            group_url = f"https://www.facebook.com/groups/{group_slug}/"
    elif group_match:
        # Standard group URL — proceed as-is
        pass
    else:
        return {"error": "Please paste a valid Facebook group or post link."}

    _cleanup_old_jobs()

    job_id = uuid.uuid4().hex[:8]
    _scrape_jobs[job_id] = {"status": "queued", "result": None, "error": None, "_ts": time.time()}

    loop = asyncio.get_running_loop()
    loop.run_in_executor(
        _executor,
        _run_pipeline, job_id, group_url, req.period, req.city, max_posts,
    )

    return {"job_id": job_id, "status": "queued"}


@app.get("/api/scrape/{job_id}")
async def api_scrape_status(job_id: str):
    """Poll scrape job status."""
    job = _scrape_jobs.get(job_id)
    if not job:
        return {"error": "Job not found"}
    return {"job_id": job_id, **job}


# --- Multi-source scrape endpoint ---

class MultiScrapeRequest(BaseModel):
    city: str = "Bangalore"
    areas: list[str] | None = None
    budget_min: int | None = None
    budget_max: int | None = None
    bedrooms: list[int] | None = None
    furnished: str = ""
    sources: list[str] | None = None  # None = all sources
    max_results: int = 500


def _run_multi_pipeline(job_id: str, req: MultiScrapeRequest):
    """Run the multi-source pipeline (blocking)."""
    try:
        def on_status(status: str):
            _scrape_jobs[job_id]["status"] = status
            _scrape_jobs[job_id]["_ts"] = time.time()

        on_status("scraping")
        params = SearchParams(
            city=req.city,
            areas=req.areas,
            budget_min=req.budget_min,
            budget_max=req.budget_max,
            bedrooms=req.bedrooms,
            furnished=req.furnished,
            max_results=req.max_results,
        )
        result = run_full_pipeline(
            params=params,
            sources=req.sources,
            skip_geocoding=False,
            on_status=on_status,
        )
        _scrape_jobs[job_id].update({
            "status": "done",
            "_ts": time.time(),
            "result": result,
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        _scrape_jobs[job_id].update({"status": "error", "_ts": time.time(), "error": str(e)})


@app.post("/api/scrape/multi")
async def api_scrape_multi(req: MultiScrapeRequest):
    """Start a multi-source scrape job."""
    _cleanup_old_jobs()

    job_id = uuid.uuid4().hex[:8]
    _scrape_jobs[job_id] = {"status": "queued", "result": None, "error": None, "_ts": time.time()}

    loop = asyncio.get_running_loop()
    loop.run_in_executor(_executor, _run_multi_pipeline, job_id, req)

    return {
        "job_id": job_id,
        "status": "queued",
        "sources": req.sources or list(ALL_SCRAPERS.keys()),
    }


@app.get("/api/sources")
async def api_sources():
    """List all available scraping sources."""
    return {
        "sources": list(ALL_SCRAPERS.keys()),
    }


# --- Message generation endpoint ---

class MessageRequest(BaseModel):
    user_name: str = ""


@app.post("/api/listings/{listing_id}/message")
async def api_generate_message(listing_id: str, req: MessageRequest):
    """Generate a broker outreach message for a specific listing."""
    from pipeline.storage import _get_conn
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM rental_listings WHERE raw_post_id = ?", (listing_id,)
        ).fetchone()
    finally:
        conn.close()

    if not row:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Listing not found")

    listing_dict = dict(row)
    message = generate_broker_message(listing_dict, user_name=req.user_name)
    return {"message": message, "listing_id": listing_id}


# --- Preferences endpoints ---

class PreferencesRequest(BaseModel):
    city: str = "Bangalore"
    areas: list[str] = []
    budget_min: int | None = None
    budget_max: int | None = None
    bedrooms: list[int] = [2, 3]
    furnished: str = ""
    listing_type: str = ""


@app.get("/api/preferences")
async def api_get_preferences():
    """Get all saved preferences."""
    prefs = get_preferences()
    return {"preferences": prefs}


@app.post("/api/preferences")
async def api_save_preferences(req: PreferencesRequest):
    """Save user preferences."""
    pref_id = save_preferences(req.model_dump())
    return {"id": pref_id, "status": "saved"}


@app.delete("/api/preferences/{pref_id}")
async def api_delete_preferences(pref_id: str):
    """Delete a preference set."""
    deleted = delete_preferences(pref_id)
    if not deleted:
        return {"error": "Preference not found"}
    return {"status": "deleted"}


# --- Scheduler integration ---

@app.on_event("startup")
async def startup_event():
    """Start the scheduler on server startup."""
    try:
        from pipeline.scheduler import setup_scheduler
        setup_scheduler()
    except Exception as e:
        print(f"  [!] Scheduler failed to start: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown the scheduler."""
    try:
        from pipeline.scheduler import shutdown_scheduler
        shutdown_scheduler()
    except Exception:
        pass


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
