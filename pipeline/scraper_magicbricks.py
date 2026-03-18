"""MagicBricks scraper using Playwright + BeautifulSoup."""

from __future__ import annotations

import json
import random
import time
from pathlib import Path

from models import RawPost
from pipeline.base_scraper import BaseScraper, SearchParams


# MagicBricks URL pattern:
# https://www.magicbricks.com/property-for-rent/residential-real-estate?bedroom=2&proptype=Multistorey-Apartment,Builder-Floor-Apartment,Penthouse,Studio-Apartment&cityName=Bangalore
# With budget: &budgetMin=20000&budgetMax=40000

CITY_NAMES = {
    "bangalore": "Bangalore",
    "mumbai": "Mumbai",
    "delhi": "New-Delhi",
    "hyderabad": "Hyderabad",
    "pune": "Pune",
    "chennai": "Chennai",
    "gurgaon": "Gurgaon",
    "noida": "Noida",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}


class MagicBricksScraper(BaseScraper):
    """Scrape rental listings from MagicBricks using Playwright."""

    source_name = "magicbricks"

    def scrape(self, params: SearchParams) -> list[RawPost]:
        """Scrape MagicBricks for rental listings."""
        city_name = CITY_NAMES.get(params.city.lower().strip())
        if not city_name:
            print(f"  [!] MagicBricks: City '{params.city}' not supported.")
            return []

        urls = self._build_search_urls(params, city_name)
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
                        time.sleep(random.uniform(2, 4))
                    except Exception as e:
                        print(f"  [!] MagicBricks: Error scraping {url}: {e}")

                browser.close()

        except ImportError:
            print("  [!] MagicBricks: Playwright not installed. Loading mock data.")
            return self._load_mock_data()

        # Fallback to mock data if scraping returned nothing
        if not all_posts:
            print("  [MagicBricks] No results from scraping, falling back to mock data")
            all_posts = self._load_mock_data()

        print(f"  [MagicBricks] Found {len(all_posts)} listings for {params.city}")
        return all_posts[:params.max_results]

    def _build_search_urls(self, params: SearchParams, city_name: str) -> list[str]:
        """Build MagicBricks search URLs."""
        urls = []
        bedrooms_list = params.bedrooms or [2, 3]

        for bhk in bedrooms_list:
            base = "https://www.magicbricks.com/property-for-rent/residential-real-estate"
            query_parts = [
                f"bedroom={bhk}",
                "proptype=Multistorey-Apartment,Builder-Floor-Apartment,Penthouse,Studio-Apartment",
                f"cityName={city_name}",
            ]

            if params.budget_min:
                query_parts.append(f"budgetMin={params.budget_min}")
            if params.budget_max:
                query_parts.append(f"budgetMax={params.budget_max}")

            # Area/locality filter
            if params.areas:
                locality = params.areas[0].replace(" ", "-")
                query_parts.append(f"keyword={locality}")

            url = f"{base}?{'&'.join(query_parts)}"
            urls.append(url)

        return urls

    def _scrape_page(self, page, url: str, BeautifulSoup) -> list[RawPost]:
        """Scrape a single MagicBricks search results page."""
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")
        posts: list[RawPost] = []

        # MagicBricks listing cards
        listing_cards = (
            soup.select("[data-id]") or
            soup.select(".mb-srp__card") or
            soup.select("[class*='SRCard']") or
            soup.select("[class*='propertyCard']")
        )

        for card in listing_cards:
            post = self._parse_listing_card(card, url)
            if post:
                posts.append(post)

        return posts

    def _parse_listing_card(self, card, page_url: str) -> RawPost | None:
        """Parse a MagicBricks listing card into RawPost."""
        try:
            listing_id = card.get("data-id", card.get("id", ""))
            if not listing_id:
                listing_id = str(hash(card.get_text()[:100]))

            text_parts = []

            # Title/heading
            title = card.select_one("h2, [class*='heading'], [class*='title']")
            if title:
                text_parts.append(title.get_text(strip=True))

            # Price
            price_elem = card.select_one("[class*='price'], [class*='rent']")
            if price_elem:
                text_parts.append(f"Rent: {price_elem.get_text(strip=True)}")

            # BHK config
            config_elem = card.select_one("[class*='config'], [class*='bhk'], [class*='bed']")
            if config_elem:
                text_parts.append(config_elem.get_text(strip=True))

            # Location
            loc_elem = card.select_one("[class*='locality'], [class*='address'], [class*='location']")
            if loc_elem:
                text_parts.append(loc_elem.get_text(strip=True))

            # Area sqft
            area_elem = card.select_one("[class*='area'], [class*='size'], [class*='sqft']")
            if area_elem:
                text_parts.append(area_elem.get_text(strip=True))

            # Description/amenities
            desc_elem = card.select_one("[class*='desc'], [class*='detail'], [class*='amenity']")
            if desc_elem:
                text_parts.append(desc_elem.get_text(strip=True))

            text = "\n".join(text_parts) if text_parts else card.get_text(strip=True)[:500]
            if not text:
                return None

            # Link
            link = card.select_one("a[href*='rent']") or card.select_one("a[href]")
            post_url = ""
            if link and link.get("href"):
                href = link["href"]
                if href.startswith("/"):
                    post_url = f"https://www.magicbricks.com{href}"
                elif href.startswith("http"):
                    post_url = href

            # Images
            images = []
            for img in card.select("img[src]"):
                src = img.get("src", "")
                if src and "magicbricks" in src and "logo" not in src:
                    images.append(src)

            return self._make_raw_post(
                post_id=listing_id,
                text=text,
                image_urls=images,
                post_url=post_url,
                source_listing_id=listing_id,
            )

        except Exception as e:
            print(f"  [!] MagicBricks: Failed to parse card: {e}")
            return None

    def _load_mock_data(self) -> list[RawPost]:
        """Load mock data for development."""
        mock_path = Path("sample_data/mock_magicbricks.json")
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
