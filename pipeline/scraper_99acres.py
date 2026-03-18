"""99acres scraper using Playwright + BeautifulSoup."""

from __future__ import annotations

import json
import random
import time
from pathlib import Path

from models import RawPost, RentalListing
from pipeline.base_scraper import BaseScraper, SearchParams


# 99acres URL pattern: https://www.99acres.com/{bhk}-bhk-flats-for-rent-in-{city}-ffid
# With area: https://www.99acres.com/{bhk}-bhk-flats-for-rent-in-{area}-{city}-ffid
# With price: https://www.99acres.com/{bhk}-bhk-flats-for-rent-in-{city}-{min}-thousand-to-{max}-thousand-ffid

CITY_SLUGS = {
    "bangalore": "bangalore",
    "mumbai": "mumbai",
    "delhi": "new-delhi",
    "hyderabad": "hyderabad",
    "pune": "pune",
    "chennai": "chennai",
    "gurgaon": "gurgaon",
    "noida": "noida",
}

# Common area slugs for 99acres
AREA_SLUGS = {
    "koramangala": "koramangala-bangalore-south",
    "hsr layout": "hsr-layout-bangalore-south",
    "indiranagar": "indiranagar-bangalore-east",
    "whitefield": "whitefield-bangalore-east",
    "marathahalli": "marathahalli-bangalore-east",
    "electronic city": "electronic-city-bangalore-south",
    "btm layout": "btm-layout-bangalore-south",
    "jayanagar": "jayanagar-bangalore-south",
    "bellandur": "bellandur-bangalore-east",
    "jp nagar": "jp-nagar-bangalore-south",
    "sarjapur road": "sarjapur-road-bangalore-east",
    "andheri": "andheri-west-mumbai",
    "bandra": "bandra-west-mumbai",
    "powai": "powai-mumbai",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


class NinetyNineAcresScraper(BaseScraper):
    """Scrape rental listings from 99acres using Playwright."""

    source_name = "99acres"

    def scrape(self, params: SearchParams) -> list[RawPost]:
        """Scrape 99acres for rental listings."""
        city_slug = CITY_SLUGS.get(params.city.lower().strip())
        if not city_slug:
            print(f"  [!] 99acres: City '{params.city}' not supported.")
            return []

        urls = self._build_search_urls(params, city_slug)
        all_posts: list[RawPost] = []

        try:
            from playwright.sync_api import sync_playwright
            from bs4 import BeautifulSoup

            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent=HEADERS["User-Agent"],
                    viewport={"width": 1280, "height": 800},
                )
                page = context.new_page()

                for url in urls:
                    try:
                        posts = self._scrape_page(page, url, BeautifulSoup)
                        all_posts.extend(posts)
                        if len(all_posts) >= params.max_results:
                            break
                        # Polite delay
                        time.sleep(random.uniform(2, 4))
                    except Exception as e:
                        print(f"  [!] 99acres: Error scraping {url}: {e}")

                browser.close()

        except ImportError:
            print("  [!] 99acres: Playwright not installed. Loading mock data.")
            return self._load_mock_data()

        # Fallback to mock data if scraping returned nothing
        if not all_posts:
            print("  [99acres] No results from scraping, falling back to mock data")
            all_posts = self._load_mock_data()

        print(f"  [99acres] Found {len(all_posts)} listings for {params.city}")
        return all_posts[:params.max_results]

    def _build_search_urls(self, params: SearchParams, city_slug: str) -> list[str]:
        """Build 99acres search URLs from parameters."""
        urls = []
        bedrooms_list = params.bedrooms or [2, 3]

        for bhk in bedrooms_list:
            # Base URL
            if params.areas:
                for area in params.areas:
                    area_slug = AREA_SLUGS.get(area.lower().strip())
                    if area_slug:
                        url = f"https://www.99acres.com/{bhk}-bhk-flats-for-rent-in-{area_slug}-ffid"
                    else:
                        # Try constructing slug from area name
                        area_clean = area.lower().strip().replace(" ", "-")
                        url = f"https://www.99acres.com/{bhk}-bhk-flats-for-rent-in-{area_clean}-{city_slug}-ffid"
                    urls.append(url)
            else:
                url = f"https://www.99acres.com/{bhk}-bhk-flats-for-rent-in-{city_slug}-ffid"
                urls.append(url)

        return urls

    def _scrape_page(self, page, url: str, BeautifulSoup) -> list[RawPost]:
        """Scrape a single 99acres search results page."""
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        # Wait for listings to load
        page.wait_for_timeout(3000)

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")
        posts: list[RawPost] = []

        # 99acres listing cards — try multiple selectors
        # They use various class patterns, so we try several
        listing_cards = (
            soup.select("[data-listing-id]") or
            soup.select(".srpTuple__tupleTable") or
            soup.select(".projectTuple") or
            soup.select("[class*='tupleNew']") or
            soup.select("[class*='listingCard']")
        )

        for card in listing_cards:
            post = self._parse_listing_card(card, url)
            if post:
                posts.append(post)

        return posts

    def _parse_listing_card(self, card, page_url: str) -> RawPost | None:
        """Parse a single 99acres listing card into RawPost."""
        try:
            # Extract listing ID
            listing_id = card.get("data-listing-id", "")
            if not listing_id:
                # Try finding it in a child element
                id_elem = card.select_one("[data-listing-id]")
                if id_elem:
                    listing_id = id_elem.get("data-listing-id", "")
            if not listing_id:
                listing_id = str(hash(card.get_text()[:100]))

            # Extract text content
            text_parts = []

            # Title
            title = card.select_one("h2, [class*='heading'], [class*='title']")
            if title:
                text_parts.append(title.get_text(strip=True))

            # Config (2BHK, etc.)
            config_elem = card.select_one("[class*='config'], [class*='bhk']")
            if config_elem:
                text_parts.append(config_elem.get_text(strip=True))

            # Price
            price_elem = card.select_one("[class*='price'], [class*='rent']")
            if price_elem:
                text_parts.append(f"Rent: {price_elem.get_text(strip=True)}")

            # Location
            loc_elem = card.select_one("[class*='locality'], [class*='address'], [class*='location']")
            if loc_elem:
                text_parts.append(loc_elem.get_text(strip=True))

            # Area (sqft)
            area_elem = card.select_one("[class*='area'], [class*='size']")
            if area_elem:
                text_parts.append(area_elem.get_text(strip=True))

            # Description
            desc_elem = card.select_one("[class*='desc'], [class*='amenity']")
            if desc_elem:
                text_parts.append(desc_elem.get_text(strip=True))

            text = "\n".join(text_parts) if text_parts else card.get_text(strip=True)[:500]
            if not text:
                return None

            # Extract link
            link = card.select_one("a[href*='/property/']") or card.select_one("a[href]")
            post_url = ""
            if link and link.get("href"):
                href = link["href"]
                if href.startswith("/"):
                    post_url = f"https://www.99acres.com{href}"
                elif href.startswith("http"):
                    post_url = href

            # Extract images
            images = []
            for img in card.select("img[src]"):
                src = img.get("src", "")
                if src and "99acres" in src and "logo" not in src:
                    images.append(src)

            return self._make_raw_post(
                post_id=listing_id,
                text=text,
                image_urls=images,
                post_url=post_url,
                source_listing_id=listing_id,
            )

        except Exception as e:
            print(f"  [!] 99acres: Failed to parse card: {e}")
            return None

    def _load_mock_data(self) -> list[RawPost]:
        """Load mock data for development."""
        mock_path = Path("sample_data/mock_99acres.json")
        if not mock_path.exists():
            return []
        with open(mock_path) as f:
            items = json.load(f)
        posts = []
        for item in items:
            posts.append(self._make_raw_post(
                post_id=item["id"],
                text=item["text"],
                post_url=item.get("url", ""),
                image_urls=item.get("images", []),
                source_listing_id=item["id"],
            ))
        return posts
