"""Housing.com scraper — uses httpx + BeautifulSoup with Apify fallback.

Housing.com uses Akamai bot protection, making direct API calls difficult.
Strategy:
1. Try direct HTTP scraping (works sometimes, depends on Akamai mood)
2. Fall back to Apify if HTTP scraping is blocked (403)
3. Fall back to mock data if both fail
"""

from __future__ import annotations

import json
import random
import re
import time
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

import config
from models import RawPost
from pipeline.base_scraper import BaseScraper, SearchParams


CITY_SLUGS = {
    "bangalore": {"slug": "bangalore", "city_id": "1"},
    "mumbai": {"slug": "mumbai", "city_id": "2"},
    "delhi": {"slug": "new-delhi", "city_id": "3"},
    "hyderabad": {"slug": "hyderabad", "city_id": "38"},
    "pune": {"slug": "pune", "city_id": "5"},
    "chennai": {"slug": "chennai", "city_id": "6"},
}

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]


def _get_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://housing.com/",
        "Connection": "keep-alive",
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

        all_posts: list[RawPost] = []

        # Strategy 1: Try direct HTTP scraping
        print(f"  [Housing.com] Trying direct HTTP scraping...")
        all_posts = self._scrape_via_http(params, city_config["slug"])

        # Strategy 2: If blocked, try Apify
        if not all_posts and config.APIFY_API_TOKEN:
            print(f"  [Housing.com] HTTP blocked, trying Apify...")
            all_posts = self._scrape_via_apify(params, city_config["slug"])

        # Strategy 3: Fall back to mock data
        if not all_posts:
            print("  [Housing.com] No results, falling back to mock data")
            all_posts = self._load_mock_data()

        print(f"  [Housing.com] Found {len(all_posts)} listings for {params.city}")
        return all_posts[:params.max_results]

    def _scrape_via_http(self, params: SearchParams, city_slug: str) -> list[RawPost]:
        """Try scraping Housing.com directly via HTTP."""
        urls = self._build_web_urls(params, city_slug)
        all_posts: list[RawPost] = []

        client = httpx.Client(
            headers=_get_headers(),
            follow_redirects=True,
            timeout=30,
        )

        try:
            for url in urls:
                try:
                    resp = client.get(url)

                    if resp.status_code == 403:
                        print(f"  [!] Housing.com: Blocked (403) — Akamai protection active")
                        return []  # Don't retry, go to Apify

                    if resp.status_code == 404:
                        continue

                    resp.raise_for_status()

                    posts = self._parse_html(resp.text, url)
                    all_posts.extend(posts)
                    print(f"  [Housing.com] {url} → {len(posts)} listings")

                    if len(all_posts) >= params.max_results:
                        break

                    time.sleep(random.uniform(1.5, 3.0))

                except httpx.HTTPStatusError as e:
                    print(f"  [!] Housing.com: HTTP {e.response.status_code} for {url}")
                    if e.response.status_code == 403:
                        return []  # Blocked, try Apify
                except Exception as e:
                    print(f"  [!] Housing.com: Error fetching {url}: {e}")

        finally:
            client.close()

        return all_posts

    def _scrape_via_apify(self, params: SearchParams, city_slug: str) -> list[RawPost]:
        """Scrape Housing.com via Apify actor."""
        urls = self._build_web_urls(params, city_slug)

        try:
            items = self._call_apify(
                actor_id="easyapi~housing-com-scraper",
                run_input={
                    "startUrls": [{"url": url} for url in urls[:3]],  # Limit to 3 URLs
                    "maxItems": params.max_results,
                },
                timeout=300,
            )

            posts: list[RawPost] = []
            for item in items:
                text_parts = []
                if item.get("title"):
                    text_parts.append(item["title"])
                if item.get("price"):
                    text_parts.append(f"Rent: {item['price']}")
                if item.get("location") or item.get("locality"):
                    text_parts.append(item.get("location") or item.get("locality"))
                if item.get("bhk") or item.get("configuration"):
                    text_parts.append(str(item.get("bhk") or item.get("configuration")))
                if item.get("area") or item.get("builtUpArea"):
                    text_parts.append(str(item.get("area") or item.get("builtUpArea")))
                if item.get("furnishing"):
                    text_parts.append(item["furnishing"])
                if item.get("description"):
                    text_parts.append(item["description"][:300])

                text = "\n".join(text_parts)
                if not text or len(text) < 10:
                    continue

                listing_id = str(item.get("id", item.get("propertyId", hash(text[:100]))))
                post_url = item.get("url", item.get("link", ""))

                images = []
                if item.get("images"):
                    images = item["images"][:5] if isinstance(item["images"], list) else []
                elif item.get("imageUrl"):
                    images = [item["imageUrl"]]

                posts.append(self._make_raw_post(
                    post_id=listing_id,
                    text=text,
                    image_urls=images,
                    post_url=post_url,
                    source_listing_id=listing_id,
                ))

            return posts

        except Exception as e:
            print(f"  [!] Housing.com Apify error: {e}")
            return []

    def _build_web_urls(self, params: SearchParams, city_slug: str) -> list[str]:
        """Build Housing.com search URLs."""
        urls = []
        bedrooms_list = params.bedrooms or [1, 2, 3]

        for bhk in bedrooms_list:
            base = f"https://housing.com/in/rent/real-estate-{city_slug}"
            query_parts = [f"bedrooms={bhk}"]

            if params.budget_min:
                query_parts.append(f"rent_from={params.budget_min}")
            if params.budget_max:
                query_parts.append(f"rent_to={params.budget_max}")

            if params.areas:
                query_parts.append(f"keyword={params.areas[0]}")

            url = f"{base}?{'&'.join(query_parts)}"
            urls.append(url)

        return urls

    def _parse_html(self, html: str, url: str) -> list[RawPost]:
        """Parse Housing.com HTML for listing data."""
        soup = BeautifulSoup(html, "html.parser")
        posts: list[RawPost] = []

        # Try JSON-LD structured data first (most reliable)
        jsonld_posts = self._extract_from_jsonld(soup)
        if jsonld_posts:
            return jsonld_posts

        # Try __NEXT_DATA__ (Housing.com may use Next.js)
        next_posts = self._extract_from_next_data(soup)
        if next_posts:
            return next_posts

        # Fall back to HTML card parsing
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

    def _extract_from_jsonld(self, soup: BeautifulSoup) -> list[RawPost]:
        """Extract listings from JSON-LD structured data."""
        posts = []
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(script.string)
                items = []
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict) and data.get("@type") == "ItemList":
                    items = data.get("itemListElement", [])
                elif isinstance(data, dict) and "Residence" in str(data.get("@type", "")):
                    items = [data]

                for item in items:
                    listing = item.get("item", item)
                    name = listing.get("name", "")
                    desc = listing.get("description", "")
                    url = listing.get("url", "")

                    text_parts = [p for p in [name, desc] if p]
                    text = "\n".join(text_parts)
                    if not text or len(text) < 10:
                        continue

                    listing_id = url.split("/")[-1] if url else str(hash(text[:100]))
                    if not url.startswith("http"):
                        url = f"https://housing.com{url}" if url else ""

                    posts.append(self._make_raw_post(
                        post_id=listing_id,
                        text=text,
                        post_url=url,
                        source_listing_id=listing_id,
                    ))
            except (json.JSONDecodeError, TypeError):
                continue
        return posts

    def _extract_from_next_data(self, soup: BeautifulSoup) -> list[RawPost]:
        """Extract from __NEXT_DATA__ if Housing.com uses Next.js."""
        posts = []
        script = soup.select_one('script#__NEXT_DATA__')
        if not script or not script.string:
            return posts

        try:
            data = json.loads(script.string)
            props = data.get("props", {}).get("pageProps", {})
            listings = (
                props.get("listings", []) or
                props.get("searchResults", []) or
                props.get("properties", []) or
                props.get("initialData", {}).get("listings", [])
            )

            for item in listings:
                text_parts = []
                for key in ["title", "name"]:
                    if item.get(key):
                        text_parts.append(item[key])
                        break
                if item.get("price") or item.get("rent"):
                    text_parts.append(f"Rent: {item.get('price') or item.get('rent')}")
                if item.get("locality") or item.get("location"):
                    text_parts.append(item.get("locality") or item.get("location"))
                if item.get("bhk") or item.get("bedrooms"):
                    text_parts.append(f"{item.get('bhk') or item.get('bedrooms')} BHK")

                text = "\n".join(text_parts)
                if not text:
                    continue

                lid = str(item.get("id", item.get("propertyId", hash(text[:100]))))
                posts.append(self._make_raw_post(
                    post_id=lid,
                    text=text,
                    post_url=item.get("url", ""),
                    source_listing_id=lid,
                ))
        except (json.JSONDecodeError, TypeError):
            pass

        return posts

    def _parse_card(self, card) -> RawPost | None:
        """Parse a Housing.com listing card from HTML."""
        try:
            text_parts = []

            title = card.select_one("h2, h3, [class*='title'], [class*='heading']")
            if title:
                text_parts.append(title.get_text(strip=True))

            price_elem = card.select_one("[class*='price'], [class*='rent']")
            if price_elem:
                text_parts.append(f"Rent: {price_elem.get_text(strip=True)}")

            loc_elem = card.select_one("[class*='locality'], [class*='location'], [class*='address']")
            if loc_elem:
                text_parts.append(loc_elem.get_text(strip=True))

            config_elem = card.select_one("[class*='config'], [class*='bhk'], [class*='bedroom']")
            if config_elem:
                text_parts.append(config_elem.get_text(strip=True))

            area_elem = card.select_one("[class*='area'], [class*='size']")
            if area_elem:
                text_parts.append(area_elem.get_text(strip=True))

            text = "\n".join(text_parts) if text_parts else card.get_text(strip=True)[:500]
            if not text or len(text) < 10:
                return None

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
                parts = post_url.rstrip("/").split("/")
                listing_id = parts[-1] if parts else ""
            if not listing_id:
                listing_id = str(hash(text[:100]))

            images = []
            for img in card.select("img[src], img[data-src]"):
                src = img.get("data-src") or img.get("src", "")
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
