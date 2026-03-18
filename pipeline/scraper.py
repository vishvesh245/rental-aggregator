"""Apify Facebook Group scraper integration."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx

import config
from models import RawPost


APIFY_ACTOR_ID = "apify~facebook-groups-scraper"
APIFY_BASE_URL = "https://api.apify.com/v2"


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
        # Skip error items
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


def scrape_facebook_group(
    group_url: str,
    time_period: str = "7d",
    max_posts: int = 100,
) -> list[RawPost]:
    """
    Scrape posts from a Facebook group.

    If APIFY_API_TOKEN is not set, loads mock data from sample_data/mock_posts.json.
    """
    if not config.APIFY_API_TOKEN:
        return _load_mock_data(time_period)

    cutoff = _parse_time_period(time_period)

    run_input = {
        "startUrls": [{"url": group_url}],
        "resultsLimit": max_posts,
    }

    # Start the actor run and wait for it to finish
    url = f"{APIFY_BASE_URL}/acts/{APIFY_ACTOR_ID}/run-sync-get-dataset-items"
    resp = httpx.post(
        url,
        json=run_input,
        params={"token": config.APIFY_API_TOKEN},
        timeout=300,
    )
    resp.raise_for_status()
    items = resp.json()

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
        filtered.append(post)

    return filtered


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
        ))

    return posts
