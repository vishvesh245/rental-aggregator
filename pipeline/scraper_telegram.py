"""Telegram channel scraper — scrapes public Telegram channels via t.me/s/CHANNEL.

No API key, no login, no Apify. Completely free.
Uses the public channel preview pages that Telegram exposes for every public channel.

Strategy: Only keep posts that contain a price. Skip "WhatsApp for details" posts
that lack pricing data — they waste Claude extraction tokens and produce useless listings.
"""

from __future__ import annotations

import re
import time
import random
from datetime import datetime, timezone

import httpx
from bs4 import BeautifulSoup

from models import RawPost
from pipeline.base_scraper import BaseScraper, SearchParams


# Channels prioritized by price-inclusion rate (higher = better)
BANGALORE_CHANNELS = [
    "housingourbengaluru",        # ~7K subs, ~15% posts have prices
    "HousingBangalore",           # ~26K subs, ~10% posts have prices
    "FlatsAndFlatmatesBangalore", # ~4.6K members
    "Bangalorehousing",           # active but mostly sales — filter hard
]

CITY_CHANNELS: dict[str, list[str]] = {
    "bangalore": BANGALORE_CHANNELS,
    "mumbai": ["MumbaiFlatRent", "mumbaihousing"],
    "delhi": ["DelhiRentalFlats"],
    "hyderabad": ["HyderabadHousing"],
    "pune": ["PuneRentalFlats"],
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ─── Price detection — the core filter ───
# Only keep posts where we can find a rent amount
_PRICE_PATTERN = re.compile(
    r'(?:'
    r'(?:₹|rs\.?|inr|rent\s*[:=]?\s*)\s*[\d,.]+'  # ₹15,000 / Rs.15000 / Rent: 15000
    r'|[\d,.]+\s*(?:k|K)\b'                          # 15K / 15k
    r'|\d{4,6}\s*(?:/\s*(?:month|mo)|per\s*month|pm\b|/-)'  # 15000/month
    r')',
    re.IGNORECASE,
)

# Basic rental relevance check
_RENTAL_KEYWORDS = re.compile(
    r'\b(bhk|rent|flat|apartment|room|flatmate|pg|studio|1rk|'
    r'furnished|semi.?furnished|deposit|per\s*month|available)\b',
    re.IGNORECASE,
)

# Sale listing filter — skip these
_SALE_KEYWORDS = re.compile(
    r'\b(for\s+sale|resale|selling|buy|purchase|emi\b|crore|cr\b|'
    r'per\s*sq\s*ft|sq\s*ft\s*rate|investment|plot|land|site)\b',
    re.IGNORECASE,
)


class TelegramScraper(BaseScraper):
    """Scrape rental listings from public Telegram channels.

    Key difference from v1: Only returns posts that contain a price.
    This means fewer but higher-quality results that Claude can actually extract.
    """

    source_name = "telegram"

    def scrape(self, params: SearchParams) -> list[RawPost]:
        city = params.city.lower().strip()
        channels = CITY_CHANNELS.get(city, BANGALORE_CHANNELS)

        all_posts: list[RawPost] = []
        total_scanned = 0

        for channel in channels:
            try:
                posts, scanned = self._scrape_channel(channel, params)
                total_scanned += scanned
                print(f"  [Telegram] @{channel} → {len(posts)} with price (scanned {scanned})")
                all_posts.extend(posts)
                if len(all_posts) >= params.max_results:
                    break
                time.sleep(random.uniform(0.8, 1.5))
            except Exception as e:
                print(f"  [!] Telegram @{channel} error: {e}")

        print(f"  [Telegram] Total: {len(all_posts)} quality posts (from {total_scanned} scanned)")
        return all_posts[:params.max_results]

    def _scrape_channel(self, channel: str, params: SearchParams) -> tuple[list[RawPost], int]:
        """Scrape a single public Telegram channel. Returns (posts_with_price, total_scanned)."""
        url = f"https://t.me/s/{channel}"
        posts: list[RawPost] = []
        scanned = 0

        try:
            resp = httpx.get(url, headers=HEADERS, timeout=20, follow_redirects=True)
            if resp.status_code != 200:
                print(f"  [!] Telegram @{channel}: HTTP {resp.status_code}")
                return [], 0

            soup = BeautifulSoup(resp.text, "html.parser")
            messages = soup.select(".tgme_widget_message")

            for msg in messages:
                post = self._parse_message(msg, channel)
                if not post:
                    continue
                scanned += 1

                # THE KEY FILTER: only keep posts with a price
                if not _PRICE_PATTERN.search(post.text):
                    continue

                # Must be rental-related
                if not _RENTAL_KEYWORDS.search(post.text):
                    continue

                # Skip sale listings
                if _SALE_KEYWORDS.search(post.text):
                    continue

                # Area filter
                if params.areas and not self._matches_area(post.text, params.areas):
                    continue

                posts.append(post)

        except Exception as e:
            print(f"  [!] Telegram @{channel} fetch error: {e}")

        return posts, scanned

    def _matches_area(self, text: str, areas: list[str]) -> bool:
        """Check if post mentions any of the requested areas."""
        text_lower = text.lower()
        for area in areas:
            if area.lower() in text_lower:
                return True
        # Post doesn't mention a specific area at all — include it (city-wide)
        known_areas = re.compile(
            r'\b(koramangala|hsr|indiranagar|whitefield|marathahalli|'
            r'electronic city|bellandur|sarjapur|btm|jayanagar|hebbal|'
            r'yelahanka|kr puram|jp nagar|munnekollal|kasavanahalli|'
            r'kundanhalli|bommanahalli|banashankari|rajajinagar|'
            r'malleswaram|basavanagudi|wilson garden|richmond)\b',
            re.IGNORECASE
        )
        if known_areas.search(text):
            return False  # Has a specific area but not the one user wants
        return True  # No area mentioned — keep it

    def _parse_message(self, msg, channel: str) -> RawPost | None:
        """Parse a single Telegram message into a RawPost."""
        try:
            text_el = msg.select_one(".tgme_widget_message_text")
            text = text_el.get_text(separator="\n", strip=True) if text_el else ""
            if not text or len(text) < 30:
                return None

            msg_link = msg.select_one(".tgme_widget_message_date")
            post_url = ""
            post_id = ""
            if msg_link and msg_link.get("href"):
                post_url = msg_link["href"]
                id_match = re.search(r'/(\d+)$', post_url)
                post_id = id_match.group(1) if id_match else str(hash(text[:80]))

            time_el = msg.select_one("time")
            timestamp = time_el.get("datetime", "") if time_el else ""

            images = []
            for img in msg.select(".tgme_widget_message_photo_wrap"):
                style = img.get("style", "")
                img_match = re.search(r"url\('([^']+)'\)", style)
                if img_match:
                    images.append(img_match.group(1))

            if not post_id:
                post_id = str(hash(text[:80]))

            return self._make_raw_post(
                post_id=f"{channel}_{post_id}",
                text=text,
                timestamp=timestamp,
                image_urls=images,
                post_url=post_url,
                group_url=f"https://t.me/{channel}",
                source_listing_id=post_id,
            )

        except Exception as e:
            print(f"  [!] Telegram: Failed to parse message: {e}")
            return None
