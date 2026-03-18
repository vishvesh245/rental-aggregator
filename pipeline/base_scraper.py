"""Abstract base class for all rental listing scrapers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import httpx

import config
from models import RawPost


@dataclass
class SearchParams:
    """User search criteria passed to all scrapers."""
    city: str = "Bangalore"
    areas: list[str] | None = None        # e.g., ["Koramangala", "HSR Layout"]
    budget_min: int | None = None
    budget_max: int | None = None
    bedrooms: list[int] | None = None     # e.g., [2, 3]
    furnished: str = ""                    # "furnished", "semi-furnished", "unfurnished", ""
    listing_type: str = ""                 # "full_flat", "flatmate", "pg", ""
    max_results: int = 50


class BaseScraper(ABC):
    """Abstract base class for all scrapers."""

    source_name: str = ""  # e.g., "facebook", "nobroker", "99acres"

    @abstractmethod
    def scrape(self, params: SearchParams) -> list[RawPost]:
        """
        Scrape listings from this source based on search parameters.
        Returns a list of RawPost objects with `source` populated.
        """
        ...

    def _call_apify(self, actor_id: str, run_input: dict, timeout: int = 300) -> list[dict]:
        """Shared Apify actor invocation. Returns the dataset items."""
        if not config.APIFY_API_TOKEN:
            return []

        url = f"https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items"
        resp = httpx.post(
            url,
            json=run_input,
            params={"token": config.APIFY_API_TOKEN},
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def _make_raw_post(
        self,
        post_id: str,
        text: str,
        timestamp: str = "",
        author: str = "",
        image_urls: list[str] | None = None,
        post_url: str = "",
        group_url: str = "",
        source_listing_id: str = "",
    ) -> RawPost:
        """Create a RawPost with source fields populated."""
        return RawPost(
            post_id=f"{self.source_name}_{post_id}",
            text=text,
            timestamp=timestamp,
            author=author,
            image_urls=image_urls or [],
            post_url=post_url,
            group_url=group_url,
            source=self.source_name,
            source_listing_id=source_listing_id,
        )
