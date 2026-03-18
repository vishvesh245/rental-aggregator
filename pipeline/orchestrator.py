"""Orchestrator — runs all scrapers, extracts, deduplicates, and stores listings."""

from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from models import RawPost, RentalListing


# ─── Sale listing detection ───
# Price above this is almost certainly a sale listing, not rent (per month)
MAX_RENT_DEFAULT = 150000  # ₹1.5L/month — covers luxury rentals, filters sale prices

SALE_KEYWORDS = re.compile(
    r'\b(for\s+sale|resale|selling|buy|purchase|registry|possession|emi\b|loan\b|'
    r'cr\b|crore|lakh\s*(only|price)|per\s*sq\s*ft|sq\s*ft\s*rate|'
    r'capital\s*gain|investment|appreciate|roi\b)',
    re.IGNORECASE,
)


def sanitize_listings(listings: list[RentalListing], max_rent: int = MAX_RENT_DEFAULT) -> list[RentalListing]:
    """
    Remove likely sale listings and bad data from scraped results.
    - Filters out listings with price above max_rent (likely sale, not rent)
    - Filters out listings whose text contains sale-related keywords
    """
    clean = []
    removed = 0
    for listing in listings:
        # Price sanity check
        if listing.price and listing.price > max_rent:
            removed += 1
            continue
        # Sale keyword check in location_text or raw text
        text_to_check = " ".join(filter(None, [
            getattr(listing, 'location_text', ''),
            getattr(listing, 'area', ''),
        ]))
        if SALE_KEYWORDS.search(text_to_check):
            removed += 1
            continue
        clean.append(listing)
    if removed:
        print(f"  [Sanitize] Removed {removed} likely sale/overpriced listings (cap: ₹{max_rent:,})")
    return clean
from pipeline.base_scraper import BaseScraper, SearchParams
from pipeline.scraper_facebook import FacebookScraper
from pipeline.scraper_nobroker import NoBrokerScraper
from pipeline.scraper_99acres import NinetyNineAcresScraper
from pipeline.scraper_housing import HousingComScraper
from pipeline.extractor import extract_rental_details
from pipeline.geocoder import geocode_listings
from pipeline.storage import init_db, save_raw_posts, save_listings


# All available scrapers
ALL_SCRAPERS: dict[str, type[BaseScraper]] = {
    "facebook": FacebookScraper,
    "nobroker": NoBrokerScraper,
    "99acres": NinetyNineAcresScraper,
    "housing": HousingComScraper,
}

# Sources that return structured data (no Claude extraction needed)
STRUCTURED_SOURCES = {"nobroker", "99acres", "housing"}

# Sources that return free-text (need Claude extraction)
UNSTRUCTURED_SOURCES = {"facebook"}


def run_full_pipeline(
    params: SearchParams,
    sources: list[str] | None = None,
    skip_geocoding: bool = False,
) -> dict:
    """
    Run the full scrape → extract → geocode → store pipeline for all sources.

    Args:
        params: Search parameters (city, budget, BHK, etc.)
        sources: Which sources to scrape. None = all sources.
        skip_geocoding: Skip geocoding step (for faster testing).

    Returns:
        Summary dict with counts per source.
    """
    init_db()
    sources_to_run = sources or list(ALL_SCRAPERS.keys())

    print(f"\n{'='*60}")
    print(f"  House Hunting Agent — Multi-Source Pipeline")
    print(f"  City: {params.city}")
    print(f"  Sources: {', '.join(sources_to_run)}")
    print(f"{'='*60}")

    results = {
        "started_at": datetime.now().isoformat(),
        "params": {
            "city": params.city,
            "budget_min": params.budget_min,
            "budget_max": params.budget_max,
            "bedrooms": params.bedrooms,
        },
        "sources": {},
        "total_posts": 0,
        "total_listings": 0,
    }

    # Step 1: Scrape all sources in parallel
    # NoBroker uses scrape_and_extract for higher quality structured extraction
    nobroker_pre_extracted: list[RentalListing] = []
    non_nb_sources = [s for s in sources_to_run if s != "nobroker"]

    print(f"\n[1/3] Scraping {len(sources_to_run)} sources in parallel...")
    all_posts_by_source = _scrape_all_parallel(params, non_nb_sources) if non_nb_sources else {}

    if "nobroker" in sources_to_run:
        nobroker_scraper = NoBrokerScraper()
        nb_posts, nb_listings = nobroker_scraper.scrape_and_extract(params)
        all_posts_by_source["nobroker"] = nb_posts
        nobroker_pre_extracted = nb_listings

    # Step 2: Extract structured data
    print(f"\n[2/3] Extracting rental details...")
    all_listings: list[RentalListing] = []

    for source_name, posts in all_posts_by_source.items():
        if not posts:
            results["sources"][source_name] = {"posts": 0, "listings": 0, "status": "no_results"}
            continue

        # Save raw posts
        saved_posts = save_raw_posts(posts)
        results["total_posts"] += len(posts)

        if source_name == "nobroker" and nobroker_pre_extracted:
            # Use pre-extracted structured listings (high quality, no regex needed)
            print(f"  [{source_name}] Using structured API extraction ({len(nobroker_pre_extracted)} listings)...")
            listings = nobroker_pre_extracted
        elif source_name in UNSTRUCTURED_SOURCES:
            # Facebook/Twitter — needs Claude extraction
            print(f"  [{source_name}] Extracting with Claude ({len(posts)} posts)...")
            listings = extract_rental_details(posts)
            # Set source on extracted listings
            for listing in listings:
                listing.source = source_name
        else:
            # Other structured sources — use basic extraction
            print(f"  [{source_name}] Light extraction ({len(posts)} posts)...")
            listings = _light_extract(posts, source_name)

        all_listings.extend(listings)
        results["sources"][source_name] = {
            "posts": len(posts),
            "listings": len(listings),
            "status": "ok",
        }
        print(f"  [{source_name}] {len(posts)} posts → {len(listings)} listings")

    # Step 2.5: Sanitize — remove likely sale listings and bad data
    pre_sanitize = len(all_listings)
    max_rent = params.budget_max if params.budget_max and params.budget_max > MAX_RENT_DEFAULT else MAX_RENT_DEFAULT
    all_listings = sanitize_listings(all_listings, max_rent=max_rent)
    if len(all_listings) < pre_sanitize:
        results["removed_sale_listings"] = pre_sanitize - len(all_listings)

    # Step 3: Geocode
    if not skip_geocoding and all_listings:
        print(f"\n[3/3] Geocoding {len(all_listings)} listings...")
        all_listings = geocode_listings(all_listings, default_city=params.city)
        geocoded = sum(1 for l in all_listings if l.latitude is not None)
        print(f"  → Geocoded {geocoded}/{len(all_listings)}")
    else:
        print(f"\n[3/3] Skipping geocoding")

    # Save all listings
    saved = save_listings(all_listings)
    results["total_listings"] = saved
    results["completed_at"] = datetime.now().isoformat()

    print(f"\n{'='*60}")
    print(f"  Done! {results['total_posts']} posts → {saved} listings saved")
    print(f"{'='*60}\n")

    return results


def _scrape_all_parallel(
    params: SearchParams,
    sources: list[str],
) -> dict[str, list[RawPost]]:
    """Run all scrapers in parallel using ThreadPoolExecutor."""
    results: dict[str, list[RawPost]] = {}

    def _run_scraper(source_name: str) -> tuple[str, list[RawPost]]:
        scraper_cls = ALL_SCRAPERS.get(source_name)
        if not scraper_cls:
            print(f"  [!] Unknown source: {source_name}")
            return source_name, []
        try:
            scraper = scraper_cls()
            posts = scraper.scrape(params)
            return source_name, posts
        except Exception as e:
            print(f"  [!] {source_name} scraper failed: {e}")
            return source_name, []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_run_scraper, source): source
            for source in sources
        }
        for future in as_completed(futures):
            source_name, posts = future.result()
            results[source_name] = posts

    return results


def _light_extract(posts: list[RawPost], source: str) -> list[RentalListing]:
    """
    Light extraction for structured sources.
    The text in these posts is already semi-structured (built from API fields).
    Use basic regex extraction to pull out fields.
    """
    from pipeline.extractor import _basic_extract

    listings = _basic_extract(posts)
    for listing in listings:
        listing.source = source
        listing.source_listing_id = getattr(
            next((p for p in posts if p.post_id == listing.raw_post_id), None),
            "source_listing_id", ""
        ) if posts else ""
    return listings


def run_single_source(
    source_name: str,
    params: SearchParams,
) -> list[RawPost]:
    """Run a single scraper. Useful for testing."""
    scraper_cls = ALL_SCRAPERS.get(source_name)
    if not scraper_cls:
        raise ValueError(f"Unknown source: {source_name}. Available: {list(ALL_SCRAPERS.keys())}")
    scraper = scraper_cls()
    return scraper.scrape(params)
