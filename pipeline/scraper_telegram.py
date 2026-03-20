"""Telegram channel scraper — scrapes public Telegram channels via t.me/s/CHANNEL.

No API key, no login, no Apify. Completely free.
Uses the public channel preview pages that Telegram exposes for every public channel.
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


# Active Bangalore rental channels (public, verified active as of 2026-03)
BANGALORE_CHANNELS = [
    "HousingBangalore",       # ~26,500 subscribers, multiple posts/day
    "housingourbengaluru",    # ~7,000 subscribers
    "FlatsAndFlatmatesBangalore",  # ~4,600 members
    "Bangalorehousing",       # active
]

# Other city channels (add more as needed)
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

# Filter out non-rental posts (ads, reposts, unrelated)
_RENTAL_KEYWORDS = re.compile(
    r'\b(bhk|rent|flat|apartment|room|flatmate|pg|studio|1rk|2bhk|1bhk|3bhk|'
    r'furnished|semi.?furnished|unfurnished|deposit|per\s*month|\/month|'
    r'available|immediate|koramangala|hsr|indiranagar|whitefield|'
    r'marathahalli|electronic\s*city|bellandur|sarjapur|btm|jayanagar)\b',
    re.IGNORECASE,
)


class TelegramScraper(BaseScraper):
    """Scrape rental listings from public Telegram channels."""

    source_name = "telegram"

    def scrape(self, params: SearchParams) -> list[RawPost]:
        city = params.city.lower().strip()
        channels = CITY_CHANNELS.get(city, BANGALORE_CHANNELS)

        all_posts: list[RawPost] = []

        for channel in channels:
            try:
                posts = self._scrape_channel(channel, params)
                print(f"  [Telegram] @{channel} → {len(posts)} listings")
                all_posts.extend(posts)
                if len(all_posts) >= params.max_results:
                    break
                time.sleep(random.uniform(1.0, 2.0))
            except Exception as e:
                print(f"  [!] Telegram @{channel} error: {e}")

        print(f"  [Telegram] Total: {len(all_posts)} listings")
        return all_posts[:params.max_results]

    def _scrape_channel(self, channel: str, params: SearchParams) -> list[RawPost]:
        """Scrape a single public Telegram channel."""
        url = f"https://t.me/s/{channel}"
        posts: list[RawPost] = []

        try:
            resp = httpx.get(url, headers=HEADERS, timeout=20, follow_redirects=True)
            if resp.status_code != 200:
                print(f"  [!] Telegram @{channel}: HTTP {resp.status_code}")
                return []

            soup = BeautifulSoup(resp.text, "html.parser")
            messages = soup.select(".tgme_widget_message")

            for msg in messages:
                post = self._parse_message(msg, channel)
                if post and self._is_rental_post(post.text, params):
                    posts.append(post)

        except Exception as e:
            print(f"  [!] Telegram @{channel} fetch error: {e}")

        return posts

    def _parse_message(self, msg, channel: str) -> RawPost | None:
        """Parse a single Telegram message into a RawPost."""
        try:
            # Message text
            text_el = msg.select_one(".tgme_widget_message_text")
            text = text_el.get_text(separator="\n", strip=True) if text_el else ""
            if not text or len(text) < 20:
                return None

            # Message ID and URL
            msg_link = msg.select_one(".tgme_widget_message_date")
            post_url = ""
            post_id = ""
            if msg_link and msg_link.get("href"):
                post_url = msg_link["href"]
                # Extract message ID from URL like https://t.me/HousingBangalore/1234
                id_match = re.search(r'/(\d+)$', post_url)
                post_id = id_match.group(1) if id_match else str(hash(text[:80]))

            # Timestamp
            time_el = msg.select_one("time")
            timestamp = time_el.get("datetime", "") if time_el else ""

            # Images
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

    def _is_rental_post(self, text: str, params: SearchParams) -> bool:
        """Filter to only rental-related posts matching search params."""
        if not _RENTAL_KEYWORDS.search(text):
            return False

        # Area filter — if user specified areas, check if post mentions them
        if params.areas:
            area_match = any(
                area.lower() in text.lower()
                for area in params.areas
            )
            if not area_match:
                # Still include if no specific area mentioned in post (city-wide posts)
                has_any_area = re.search(
                    r'\b(koramangala|hsr|indiranagar|whitefield|marathahalli|'
                    r'electronic city|bellandur|sarjapur|btm|jayanagar|hebbal|'
                    r'yelahanka|kr puram|jp nagar|sector|layout|nagar)\b',
                    text, re.IGNORECASE
                )
                if has_any_area:
                    return False  # Has a specific area but not the one we want

        # BHK filter
        if params.bedrooms:
            bhk_found = any(
                re.search(rf'\b{n}\s*bhk\b|\b{n}\s*bedroom', text, re.IGNORECASE)
                for n in params.bedrooms
            )
            if not bhk_found:
                # Only filter if post explicitly mentions a BHK
                has_any_bhk = re.search(r'\d\s*bhk', text, re.IGNORECASE)
                if has_any_bhk:
                    return False

        return True
