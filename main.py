#!/usr/bin/env python3
"""Rental Aggregator — CLI entry point."""

from __future__ import annotations

import argparse
import sys

from pipeline.scraper_facebook import scrape_facebook_group
from pipeline.extractor import extract_rental_details
from pipeline.geocoder import geocode_listings
from pipeline.storage import init_db, save_raw_posts, save_listings, get_listings, get_stats


def cmd_scrape(args: argparse.Namespace) -> None:
    """Run the full scrape → extract → geocode → store pipeline."""
    print(f"\n{'='*60}")
    print(f"  Rental Aggregator Pipeline")
    print(f"{'='*60}")

    # Step 1: Scrape
    print(f"\n[1/4] Scraping posts (period: {args.period})...")
    posts = scrape_facebook_group(args.group_url, time_period=args.period, max_posts=args.max_posts)
    print(f"  → Found {len(posts)} posts")

    if not posts:
        print("  No posts found. Exiting.")
        return

    # Save raw posts
    init_db()
    saved = save_raw_posts(posts)
    print(f"  → Saved {saved} raw posts to DB")

    # Step 2: Extract
    print(f"\n[2/4] Extracting rental details with Claude...")
    listings = extract_rental_details(posts)
    supply = [l for l in listings if l.post_type == "supply"]
    demand = [l for l in listings if l.post_type == "demand"]
    other = [l for l in listings if l.post_type == "other"]
    print(f"  → Extracted {len(supply)} rental listings, {len(demand)} demand posts, {len(other)} other")

    # Step 3: Geocode
    print(f"\n[3/4] Geocoding locations...")
    listings = geocode_listings(listings, default_city=args.city)
    geocoded = sum(1 for l in listings if l.latitude is not None)
    print(f"  → Geocoded {geocoded}/{len(listings)} listings")

    # Step 4: Store
    print(f"\n[4/4] Saving to database...")
    saved = save_listings(listings)
    print(f"  → Saved {saved} listings")

    print(f"\n{'='*60}")
    print(f"  Done! Run 'python main.py list' to see results.")
    print(f"{'='*60}\n")


def cmd_list(args: argparse.Namespace) -> None:
    """List stored rental listings with filters."""
    init_db()
    listings = get_listings(
        city=args.city or "",
        area=args.area or "",
        price_min=args.min_price,
        price_max=args.max_price,
        bedrooms=args.bedrooms,
        post_type="" if args.type == "all" else args.type,
    )

    if not listings:
        print("No listings found matching your filters.")
        return

    print(f"\n Found {len(listings)} listings:\n")
    print(f"{'─'*80}")

    for i, l in enumerate(listings, 1):
        price_str = f"₹{l['price']:,}/mo" if l['price'] else "Price N/A"
        beds_str = f"{l['bedrooms']}BHK" if l['bedrooms'] is not None else ""
        if l['bedrooms'] == 0:
            beds_str = "1RK/Studio"
        location = f"{l['area']}, {l['city']}" if l['area'] and l['city'] else l['area'] or l['city'] or "Location N/A"
        furnished = l['furnished'] or ""
        coords = f"  📍 ({l['latitude']:.4f}, {l['longitude']:.4f})" if l['latitude'] else ""
        contact = f"  📞 {l['contact_phone']}" if l['contact_phone'] else ""
        tag = f"  [{l['post_type'].upper()}]" if l['post_type'] != "supply" else ""

        # Date
        post_date = ""
        if l.get('post_date'):
            post_date = l['post_date'][:10]  # YYYY-MM-DD

        print(f"  {i}. {beds_str} {location} — {price_str}{tag}")
        if furnished:
            print(f"     {furnished}")
        if post_date:
            print(f"     Posted: {post_date}")
        if contact:
            print(f"    {contact}")
        if coords:
            print(f"    {coords}")
        if l.get('post_url'):
            print(f"     🔗 {l['post_url']}")

        # Show truncated original post text for verification
        raw_text = l.get('raw_text', '')
        if raw_text:
            snippet = raw_text[:120] + ("..." if len(raw_text) > 120 else "")
            print(f"     📝 \"{snippet}\"")

        print(f"{'─'*80}")


def cmd_stats(args: argparse.Namespace) -> None:
    """Show summary statistics."""
    init_db()
    stats = get_stats()

    print(f"\n{'='*50}")
    print(f"  Rental Aggregator — Stats")
    print(f"{'='*50}")
    print(f"  Raw posts scraped:    {stats['total_posts']}")
    print(f"  Rental listings:      {stats['total_listings']}")
    print(f"  Demand posts:         {stats['demand_posts']}")
    print(f"  Geocoded:             {stats['geocoded']}")

    if stats.get("price_min") is not None:
        print(f"\n  Price range:  ₹{stats['price_min']:,} — ₹{stats['price_max']:,}")
        print(f"  Average rent: ₹{stats['price_avg']:,}")

    if stats.get("by_city"):
        print(f"\n  By city:")
        for city, count in stats["by_city"].items():
            print(f"    {city}: {count}")

    print(f"{'='*50}\n")


def cmd_demo(args: argparse.Namespace) -> None:
    """Run full pipeline with mock data (no API keys needed)."""
    print("\n  Running demo with mock data...\n")
    # Override args for demo
    args.group_url = "https://facebook.com/groups/demo-rentals"
    args.period = "30d"
    args.max_posts = 100
    args.city = args.city or "Bangalore"
    cmd_scrape(args)

    # Show results
    args.area = None
    args.min_price = None
    args.max_price = None
    args.bedrooms = None
    args.type = "supply"
    cmd_list(args)

    cmd_stats(args)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rental Aggregator — Extract and organize rental listings from Facebook groups",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # scrape command
    scrape_p = subparsers.add_parser("scrape", help="Scrape a Facebook group for rental posts")
    scrape_p.add_argument("group_url", help="Facebook group URL")
    scrape_p.add_argument("--period", default="7d", help="Time period: 24h, 7d, 30d (default: 7d)")
    scrape_p.add_argument("--city", default="", help="Default city for geocoding")
    scrape_p.add_argument("--max-posts", type=int, default=100, help="Max posts to scrape (default: 100)")
    scrape_p.set_defaults(func=cmd_scrape)

    # list command
    list_p = subparsers.add_parser("list", help="List stored rental listings")
    list_p.add_argument("--city", help="Filter by city")
    list_p.add_argument("--area", help="Filter by area/locality")
    list_p.add_argument("--min-price", type=int, help="Minimum price")
    list_p.add_argument("--max-price", type=int, help="Maximum price")
    list_p.add_argument("--bedrooms", type=int, help="Number of bedrooms")
    list_p.add_argument("--type", default="supply", choices=["supply", "demand", "all"],
                        help="Post type (default: supply)")
    list_p.set_defaults(func=cmd_list)

    # stats command
    stats_p = subparsers.add_parser("stats", help="Show summary statistics")
    stats_p.set_defaults(func=cmd_stats)

    # demo command
    demo_p = subparsers.add_parser("demo", help="Run full pipeline with mock data (no API keys needed)")
    demo_p.add_argument("--city", default="", help="Default city for geocoding")
    demo_p.set_defaults(func=cmd_demo)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
