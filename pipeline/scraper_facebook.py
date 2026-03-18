"""Facebook Group scraper via Apify — implements BaseScraper."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import config
from models import RawPost
from pipeline.base_scraper import BaseScraper, SearchParams


class FacebookScraper(BaseScraper):
    """Scrape rental listings from Facebook groups using Apify."""

    source_name = "facebook"

    # Map cities to known Facebook rental groups
    CITY_GROUPS: dict[str, list[str]] = {
        "bangalore": [
            "https://www.facebook.com/groups/flatandflatematesbangalore/",
        ],
        "mumbai": [
            "https://www.facebook.com/groups/flatandflatematemumbai/",
        ],
        "delhi": [
            "https://www.facebook.com/groups/flatandflatematesdelhi/",
        ],
        "hyderabad": [
            "https://www.facebook.com/groups/flatandflatemateshyderabad/",
        ],
        "pune": [
            "https://www.facebook.com/groups/flatandflematespune/",
        ],
    }

    def scrape(self, params: SearchParams) -> list[RawPost]:
        """Scrape Facebook groups for the given city."""
        city_key = params.city.lower().strip()
        group_urls = self.CITY_GROUPS.get(city_key, [])

        if not group_urls:
            print(f"  [!] No Facebook groups configured for city: {params.city}")
            return []

        all_posts: list[RawPost] = []
        for group_url in group_urls:
            posts = self._scrape_group(group_url, max_posts=min(params.max_results, 200))
            all_posts.extend(posts)

        return all_posts

    def _scrape_group(
        self,
        group_url: str,
        time_period: str = "7d",
        max_posts: int = 50,
    ) -> list[RawPost]:
        """Scrape posts from a single Facebook group."""
        if not config.APIFY_API_TOKEN:
            return _load_mock_data(time_period)

        cutoff = _parse_time_period(time_period)

        items = self._call_apify(
            actor_id="apify~facebook-groups-scraper",
            run_input={
                "startUrls": [{"url": group_url}],
                "resultsLimit": max_posts,
            },
        )

        posts = _apify_response_to_posts(items, group_url)

        # Filter by time period
        filtered = []
        for post in posts:
            if post.timestamp:
                try:
                    post_time = datetime.fromisoformat(post.timestamp.replace("Z", "+00:00"))
                    if post_time < cutoff:
                        continue
                except ValueError:
                    pass
            # Set source fields
            post.source = self.source_name
            filtered.append(post)

        return filtered


# --- Legacy compatibility: keep the old function signature working ---

def scrape_facebook_group(
    group_url: str,
    time_period: str = "7d",
    max_posts: int = 100,
) -> list[RawPost]:
    """
    Legacy function — used by server.py and main.py.
    Delegates to FacebookScraper internally.
    """
    scraper = FacebookScraper()
    return scraper._scrape_group(group_url, time_period=time_period, max_posts=max_posts)


# --- Helper functions (moved from original scraper.py) ---

def _parse_time_period(period: str) -> datetime:
    """Convert a period string like '24h', '7d', '30d' to a cutoff datetime."""
    now = datetime.now(timezone.utc)
    unit = period[-1].lower()
    value = int(period[:-1])
    if unit == "h":
        return now - timedelta(hours=value)
    if unit == "d":
        return now - timedelta(days=value)
    raise ValueError(f"Unknown time period format: {period}. Use e.g. '24h', '7d', '30d'.")


def _apify_response_to_posts(items: list[dict], group_url: str) -> list[RawPost]:
    """Convert Apify actor output items to RawPost objects."""
    posts = []
    for item in items:
        if item.get("error"):
            continue
        post = RawPost(
            post_id=item.get("legacyId", item.get("postId", item.get("id", ""))),
            text=item.get("text", item.get("message", "")),
            timestamp=item.get("time", item.get("timestamp", "")),
            author=item.get("user", {}).get("name", "") if isinstance(item.get("user"), dict) else item.get("userName", ""),
            image_urls=_extract_image_urls(item),
            post_url=item.get("url", item.get("postUrl", "")),
            group_url=item.get("facebookUrl", group_url),
            source="facebook",
        )
        if post.text:
            posts.append(post)
    return posts


def _extract_image_urls(item: dict) -> list[str]:
    """Extract image URLs from various attachment formats."""
    urls = []
    for att in item.get("attachments", []):
        if att.get("thumbnail"):
            urls.append(att["thumbnail"])
        if att.get("image", {}).get("uri"):
            urls.append(att["image"]["uri"])
    if not urls:
        urls = item.get("images", item.get("media", [])) or []
    return urls


def _load_mock_data(time_period: str) -> list[RawPost]:
    """Load mock posts from sample data, filtered by time period."""
    with open(config.MOCK_DATA_PATH) as f:
        raw_items = json.load(f)

    cutoff = _parse_time_period(time_period)

    posts = []
    for item in raw_items:
        ts = item.get("timestamp", "")
        if ts:
            post_time = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if post_time < cutoff:
                continue

        posts.append(RawPost(
            post_id=item["post_id"],
            text=item["text"],
            timestamp=item["timestamp"],
            author=item.get("author", ""),
            image_urls=item.get("image_urls", []),
            post_url=item.get("post_url", ""),
            group_url=item.get("group_url", ""),
            source="facebook",
        ))

    return posts
