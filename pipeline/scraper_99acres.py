"""99acres scraper — Apify primary, httpx+BeautifulSoup fallback (no Playwright).

99acres blocks direct HTTP requests (403). Strategy:
1. Try Apify actor (reliable, handles anti-bot)
2. Fall back to direct HTTP with session cookies
3. Fall back to mock data
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
from models import RawPost, RentalListing
from pipeline.base_scraper import BaseScraper, SearchParams


# 99acres URL patterns for rent search
# https://www.99acres.com/2-bhk-flats-for-rent-in-bangalore-ffid
# https://www.99acres.com/2-bhk-flats-for-rent-in-koramangala-bangalore-south-ffid

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
    "hebbal": "hebbal-bangalore-north",
    "yelahanka": "yelahanka-bangalore-north",
    "kr puram": "kr-puram-bangalore-east",
    "andheri": "andheri-west-mumbai",
    "bandra": "bandra-west-mumbai",
    "powai": "powai-mumbai",
    "malad": "malad-west-mumbai",
}

# Rotate User-Agents to reduce blocking
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]


def _get_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }


# Regex patterns to extract data from 99acres HTML
_PRICE_RE = re.compile(r'₹\s*([\d,]+(?:\.\d+)?)\s*(?:/\s*month|per\s*month)?', re.IGNORECASE)
_PRICE_NUM_RE = re.compile(r'([\d,]+(?:\.\d+)?)\s*(?:K|k|Thousand|thousand|Lac|lac|L)', re.IGNORECASE)


class NinetyNineAcresScraper(BaseScraper):
    """Scrape rental listings from 99acres using HTTP requests (no browser)."""

    source_name = "99acres"

    def scrape(self, params: SearchParams) -> list[RawPost]:
        """Scrape 99acres for rental listings."""
        city_slug = CITY_SLUGS.get(params.city.lower().strip())
        if not city_slug:
            print(f"  [!] 99acres: City '{params.city}' not supported.")
            return []

        all_posts: list[RawPost] = []

        # Strategy 1: Apify (most reliable — handles anti-bot)
        if config.APIFY_API_TOKEN:
            print(f"  [99acres] Using Apify scraper...")
            all_posts = self._scrape_via_apify(params, city_slug)

        # Strategy 2: Direct HTTP (may get 403'd)
        if not all_posts:
            print(f"  [99acres] Trying direct HTTP...")
            all_posts = self._scrape_via_http(params, city_slug)

        # Strategy 3: Mock data
        if not all_posts:
            print("  [99acres] No results, falling back to mock data")
            all_posts = self._load_mock_data()

        print(f"  [99acres] Total: {len(all_posts)} listings for {params.city}")
        return all_posts[:params.max_results]

    def _scrape_via_apify(self, params: SearchParams, city_slug: str) -> list[RawPost]:
        """Scrape 99acres via Apify actor."""
        urls = self._build_search_urls(params, city_slug)

        try:
            items = self._call_apify(
                actor_id="stealth_mode~99acres-property-search-scraper",
                run_input={
                    "startUrls": [{"url": url} for url in urls[:5]],
                    "maxItems": params.max_results,
                },
                timeout=120,
            )

            posts: list[RawPost] = []
            for item in items:
                text_parts = []
                # Try common field names from 99acres Apify actors
                for key in ["title", "name", "propertyTitle"]:
                    if item.get(key):
                        text_parts.append(item[key])
                        break
                if item.get("price") or item.get("rent"):
                    text_parts.append(f"Rent: {item.get('price') or item.get('rent')}")
                if item.get("locality") or item.get("location") or item.get("address"):
                    text_parts.append(item.get("locality") or item.get("location") or item.get("address"))
                if item.get("configuration") or item.get("bhk") or item.get("bedrooms"):
                    val = item.get("configuration") or item.get("bhk") or item.get("bedrooms")
                    text_parts.append(f"{val} BHK" if str(val).isdigit() else str(val))
                if item.get("area") or item.get("builtUpArea") or item.get("superBuiltUpArea"):
                    text_parts.append(str(item.get("area") or item.get("builtUpArea") or item.get("superBuiltUpArea")))
                if item.get("furnishing") or item.get("furnished"):
                    text_parts.append(str(item.get("furnishing") or item.get("furnished")))
                if item.get("description"):
                    text_parts.append(item["description"][:300])

                text = "\n".join(text_parts)
                if not text or len(text) < 10:
                    continue

                listing_id = str(item.get("id", item.get("listingId", item.get("propertyId", hash(text[:100])))))
                post_url = item.get("url", item.get("link", item.get("propertyUrl", "")))
                if post_url and not post_url.startswith("http"):
                    post_url = f"https://www.99acres.com{post_url}"

                images = []
                if item.get("images") and isinstance(item["images"], list):
                    images = item["images"][:5]
                elif item.get("imageUrl"):
                    images = [item["imageUrl"]]

                posts.append(self._make_raw_post(
                    post_id=listing_id,
                    text=text,
                    image_urls=images,
                    post_url=post_url,
                    source_listing_id=listing_id,
                ))

            print(f"  [99acres] Apify returned {len(posts)} listings")
            return posts

        except Exception as e:
            print(f"  [!] 99acres Apify error: {e}")
            return []

    def _scrape_via_http(self, params: SearchParams, city_slug: str) -> list[RawPost]:
        """Try scraping 99acres directly via HTTP (may get blocked)."""
        urls = self._build_search_urls(params, city_slug)
        all_posts: list[RawPost] = []

        client = httpx.Client(
            headers=_get_headers(),
            follow_redirects=True,
            timeout=30,
        )

        try:
            # First hit homepage to get cookies
            try:
                client.get("https://www.99acres.com/", headers=_get_headers())
            except Exception:
                pass

            for url in urls:
                try:
                    posts = self._fetch_and_parse(client, url)
                    all_posts.extend(posts)
                    if posts:
                        print(f"  [99acres] {url} → {len(posts)} listings")

                    if len(all_posts) >= params.max_results:
                        break

                    time.sleep(random.uniform(1.5, 3.0))

                except Exception as e:
                    print(f"  [!] 99acres: Error fetching {url}: {e}")

        finally:
            client.close()

        return all_posts

    def _build_search_urls(self, params: SearchParams, city_slug: str) -> list[str]:
        """Build 99acres search URLs from parameters."""
        urls = []
        bedrooms_list = params.bedrooms or [1, 2, 3]

        for bhk in bedrooms_list:
            if params.areas:
                for area in params.areas:
                    area_slug = AREA_SLUGS.get(area.lower().strip())
                    if area_slug:
                        url = f"https://www.99acres.com/{bhk}-bhk-flats-for-rent-in-{area_slug}-ffid"
                    else:
                        area_clean = area.lower().strip().replace(" ", "-")
                        url = f"https://www.99acres.com/{bhk}-bhk-flats-for-rent-in-{area_clean}-{city_slug}-ffid"
                    urls.append(url)
            else:
                url = f"https://www.99acres.com/{bhk}-bhk-flats-for-rent-in-{city_slug}-ffid"
                urls.append(url)

        return urls

    def _fetch_and_parse(self, client: httpx.Client, url: str) -> list[RawPost]:
        """Fetch a 99acres page and parse listing cards from HTML."""
        resp = client.get(url)
        if resp.status_code == 403:
            print(f"  [!] 99acres: Blocked (403) for {url}")
            return []
        if resp.status_code == 404:
            return []
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        posts: list[RawPost] = []

        # Try multiple selectors — 99acres changes their HTML structure
        listing_cards = (
            soup.select("[data-listing-id]") or
            soup.select(".srpTuple__tupleTable") or
            soup.select(".projectTuple") or
            soup.select("[class*='tupleNew']") or
            soup.select("[class*='ListingCard']") or
            soup.select("[class*='srpTuple']")
        )

        # Also try finding listings via JSON-LD structured data
        if not listing_cards:
            posts_from_jsonld = self._extract_from_jsonld(soup)
            if posts_from_jsonld:
                return posts_from_jsonld

        # Also try __NEXT_DATA__ (if they use Next.js)
        if not listing_cards:
            posts_from_next = self._extract_from_next_data(soup)
            if posts_from_next:
                return posts_from_next

        for card in listing_cards:
            post = self._parse_listing_card(card, url)
            if post:
                posts.append(post)

        return posts

    def _extract_from_jsonld(self, soup: BeautifulSoup) -> list[RawPost]:
        """Try extracting listings from JSON-LD structured data in the page."""
        posts = []
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(script.string)
                items = []
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict) and data.get("@type") == "ItemList":
                    items = data.get("itemListElement", [])
                elif isinstance(data, dict) and "RealEstateListing" in str(data.get("@type", "")):
                    items = [data]

                for item in items:
                    listing = item.get("item", item)
                    name = listing.get("name", "")
                    desc = listing.get("description", "")
                    price = listing.get("price", "")
                    url = listing.get("url", "")
                    address = ""
                    if listing.get("address"):
                        addr = listing["address"]
                        address = addr.get("streetAddress", "") or addr.get("addressLocality", "")

                    text_parts = [p for p in [name, f"Rent: {price}" if price else "", address, desc] if p]
                    text = "\n".join(text_parts)
                    if not text or len(text) < 10:
                        continue

                    listing_id = url.split("/")[-1] if url else str(hash(text[:100]))
                    posts.append(self._make_raw_post(
                        post_id=listing_id,
                        text=text,
                        post_url=url if url.startswith("http") else f"https://www.99acres.com{url}" if url else "",
                        source_listing_id=listing_id,
                    ))
            except (json.JSONDecodeError, TypeError, KeyError):
                continue
        return posts

    def _extract_from_next_data(self, soup: BeautifulSoup) -> list[RawPost]:
        """Try extracting from __NEXT_DATA__ script tag (if 99acres uses Next.js)."""
        posts = []
        script = soup.select_one('script#__NEXT_DATA__')
        if not script or not script.string:
            return posts

        try:
            data = json.loads(script.string)
            # Navigate through Next.js data structure to find listings
            props = data.get("props", {}).get("pageProps", {})
            listings = props.get("listings", []) or props.get("searchResults", []) or props.get("properties", [])

            for item in listings:
                text_parts = []
                if item.get("title"):
                    text_parts.append(item["title"])
                if item.get("price"):
                    text_parts.append(f"Rent: {item['price']}")
                if item.get("locality") or item.get("location"):
                    text_parts.append(item.get("locality") or item.get("location"))
                if item.get("configuration") or item.get("bhk"):
                    text_parts.append(str(item.get("configuration") or item.get("bhk")))

                text = "\n".join(text_parts)
                if not text:
                    continue

                lid = str(item.get("id", item.get("listingId", hash(text[:100]))))
                posts.append(self._make_raw_post(
                    post_id=lid,
                    text=text,
                    post_url=item.get("url", ""),
                    source_listing_id=lid,
                ))
        except (json.JSONDecodeError, TypeError, KeyError):
            pass

        return posts

    def _parse_listing_card(self, card, page_url: str) -> RawPost | None:
        """Parse a single 99acres listing card into RawPost."""
        try:
            listing_id = card.get("data-listing-id", "")
            if not listing_id:
                id_elem = card.select_one("[data-listing-id]")
                if id_elem:
                    listing_id = id_elem.get("data-listing-id", "")
            if not listing_id:
                listing_id = str(hash(card.get_text()[:100]))

            text_parts = []

            # Title
            title = card.select_one("h2, h3, [class*='heading'], [class*='title'], [class*='Title']")
            if title:
                text_parts.append(title.get_text(strip=True))

            # Config (2BHK, etc.)
            config_elem = card.select_one("[class*='config'], [class*='bhk'], [class*='Config']")
            if config_elem:
                text_parts.append(config_elem.get_text(strip=True))

            # Price
            price_elem = card.select_one("[class*='price'], [class*='rent'], [class*='Price'], [class*='Rent']")
            if price_elem:
                text_parts.append(f"Rent: {price_elem.get_text(strip=True)}")

            # Location
            loc_elem = card.select_one("[class*='locality'], [class*='address'], [class*='location'], [class*='Locality']")
            if loc_elem:
                text_parts.append(loc_elem.get_text(strip=True))

            # Area (sqft)
            area_elem = card.select_one("[class*='area'], [class*='size'], [class*='Area']")
            if area_elem:
                text_parts.append(area_elem.get_text(strip=True))

            # Furnished status
            furn_elem = card.select_one("[class*='furnish'], [class*='Furnish']")
            if furn_elem:
                text_parts.append(furn_elem.get_text(strip=True))

            # Description
            desc_elem = card.select_one("[class*='desc'], [class*='amenity'], [class*='Desc']")
            if desc_elem:
                text_parts.append(desc_elem.get_text(strip=True))

            text = "\n".join(text_parts) if text_parts else card.get_text(strip=True)[:500]
            if not text or len(text) < 10:
                return None

            # Extract link
            link = card.select_one("a[href*='/property/']") or card.select_one("a[href*='rent']") or card.select_one("a[href]")
            post_url = ""
            if link and link.get("href"):
                href = link["href"]
                if href.startswith("/"):
                    post_url = f"https://www.99acres.com{href}"
                elif href.startswith("http"):
                    post_url = href

            # Extract images
            images = []
            for img in card.select("img[src], img[data-src]"):
                src = img.get("data-src") or img.get("src", "")
                if src and ("99acres" in src or "99static" in src) and "logo" not in src.lower():
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
