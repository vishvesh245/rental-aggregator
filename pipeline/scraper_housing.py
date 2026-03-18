"""Housing.com scraper — uses their internal API."""

from __future__ import annotations

import json
import random
import time
from pathlib import Path

import httpx

from models import RawPost
from pipeline.base_scraper import BaseScraper, SearchParams


# Housing.com uses an internal API for search results
# URL pattern: https://housing.com/api/gql (GraphQL) or
# https://housing.com/in/buy/search?... (web) which calls an internal API
#
# The web search URL pattern:
# https://housing.com/in/rent/bangalore/koramangala_locality-2bhk
# The API endpoint for search:
HOUSING_SEARCH_API = "https://mightyzeus.housing.com/api/v2/search/rent"

CITY_SLUGS = {
    "bangalore": {"slug": "bangalore", "city_id": "1"},
    "mumbai": {"slug": "mumbai", "city_id": "2"},
    "delhi": {"slug": "new-delhi", "city_id": "3"},
    "hyderabad": {"slug": "hyderabad", "city_id": "38"},
    "pune": {"slug": "pune", "city_id": "5"},
    "chennai": {"slug": "chennai", "city_id": "6"},
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://housing.com/",
    "Origin": "https://housing.com",
}


class HousingComScraper(BaseScraper):
    """Scrape rental listings from Housing.com."""

    source_name = "housing"

    def scrape(self, params: SearchParams) -> list[RawPost]:
        """Scrape Housing.com for rental listings."""
        city_config = CITY_SLUGS.get(params.city.lower().strip())
        if not city_config:
            print(f"  [!] Housing.com: City '{params.city}' not supported.")
            return []

        # Try the Playwright approach — scrape the web pages
        all_posts: list[RawPost] = []

        try:
            from playwright.sync_api import sync_playwright
            from bs4 import BeautifulSoup

            urls = self._build_web_urls(params, city_config["slug"])

            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent=HEADERS["User-Agent"],
                    viewport={"width": 1280, "height": 800},
                )
                page = context.new_page()

                for url in urls:
                    try:
                        posts = self._scrape_web_page(page, url, BeautifulSoup)
                        all_posts.extend(posts)
                        if len(all_posts) >= params.max_results:
                            break
                        time.sleep(random.uniform(2, 4))
                    except Exception as e:
                        print(f"  [!] Housing.com: Error scraping {url}: {e}")

                browser.close()

        except ImportError:
            print("  [!] Housing.com: Playwright not installed. Loading mock data.")
            return self._load_mock_data()

        # Fallback to mock data if scraping returned nothing
        if not all_posts:
            print("  [Housing.com] No results from scraping, falling back to mock data")
            all_posts = self._load_mock_data()

        print(f"  [Housing.com] Found {len(all_posts)} listings for {params.city}")
        return all_posts[:params.max_results]

    def _build_web_urls(self, params: SearchParams, city_slug: str) -> list[str]:
        """Build Housing.com search URLs."""
        urls = []
        bedrooms_list = params.bedrooms or [2, 3]

        for bhk in bedrooms_list:
            # Housing.com URL format: https://housing.com/in/rent/real-estate-bangalore?bedrooms=2
            base = f"https://housing.com/in/rent/real-estate-{city_slug}"
            query_parts = [f"bedrooms={bhk}"]

            if params.budget_min:
                query_parts.append(f"rent_from={params.budget_min}")
            if params.budget_max:
                query_parts.append(f"rent_to={params.budget_max}")

            if params.areas:
                # Housing.com can accept area in keyword param
                query_parts.append(f"keyword={params.areas[0]}")

            url = f"{base}?{'&'.join(query_parts)}"
            urls.append(url)

        return urls

    def _scrape_web_page(self, page, url: str, BeautifulSoup) -> list[RawPost]:
        """Scrape a Housing.com search results page."""
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(4000)

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")
        posts: list[RawPost] = []

        # Housing.com listing cards
        listing_cards = (
            soup.select("[data-test-id='property-card']") or
            soup.select("[class*='ListingCard']") or
            soup.select("[class*='property-card']") or
            soup.select("[class*='srp-card']") or
            soup.select("article") or
            soup.select("[class*='Card']")
        )

        for card in listing_cards:
            post = self._parse_card(card)
            if post:
                posts.append(post)

        return posts

    def _parse_card(self, card) -> RawPost | None:
        """Parse a Housing.com listing card."""
        try:
            text_parts = []

            # Title
            title = card.select_one("h2, h3, [class*='title'], [class*='heading']")
            if title:
                text_parts.append(title.get_text(strip=True))

            # Price
            price_elem = card.select_one("[class*='price'], [class*='rent']")
            if price_elem:
                text_parts.append(f"Rent: {price_elem.get_text(strip=True)}")

            # Location
            loc_elem = card.select_one("[class*='locality'], [class*='location'], [class*='address']")
            if loc_elem:
                text_parts.append(loc_elem.get_text(strip=True))

            # Config/BHK
            config_elem = card.select_one("[class*='config'], [class*='bhk'], [class*='bedroom']")
            if config_elem:
                text_parts.append(config_elem.get_text(strip=True))

            # Area
            area_elem = card.select_one("[class*='area'], [class*='size']")
            if area_elem:
                text_parts.append(area_elem.get_text(strip=True))

            text = "\n".join(text_parts) if text_parts else card.get_text(strip=True)[:500]
            if not text or len(text) < 10:
                return None

            # Link
            link = card.select_one("a[href*='rent']") or card.select_one("a[href]")
            post_url = ""
            if link and link.get("href"):
                href = link["href"]
                if href.startswith("/"):
                    post_url = f"https://housing.com{href}"
                elif href.startswith("http"):
                    post_url = href

            listing_id = ""
            if post_url:
                # Extract ID from URL
                parts = post_url.rstrip("/").split("/")
                listing_id = parts[-1] if parts else ""
            if not listing_id:
                listing_id = str(hash(text[:100]))

            # Images
            images = []
            for img in card.select("img[src]"):
                src = img.get("src", "")
                if src and "housing" in src and "logo" not in src:
                    images.append(src)

            return self._make_raw_post(
                post_id=listing_id,
                text=text,
                image_urls=images,
                post_url=post_url,
                source_listing_id=listing_id,
            )

        except Exception as e:
            print(f"  [!] Housing.com: Failed to parse card: {e}")
            return None

    def _load_mock_data(self) -> list[RawPost]:
        """Load mock data for development."""
        mock_path = Path("sample_data/mock_housing.json")
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
