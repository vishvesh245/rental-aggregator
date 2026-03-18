"""NoBroker scraper — hits their internal API directly."""

from __future__ import annotations

import json
import hashlib
from datetime import datetime
from pathlib import Path

import httpx

import config
from models import RawPost, RentalListing
from pipeline.base_scraper import BaseScraper, SearchParams


# NoBroker API base
NOBROKER_API = "https://www.nobroker.in/api/v3/multi/property/RENT/filter"

# City-specific search params (lat/lng centers for NoBroker's API)
CITY_CONFIGS = {
    "bangalore": {
        "city": "bangalore",
        "lat": 12.9716,
        "lng": 77.5946,
    },
    "mumbai": {
        "city": "mumbai",
        "lat": 19.0760,
        "lng": 72.8777,
    },
    "delhi": {
        "city": "delhi",
        "lat": 28.6139,
        "lng": 77.2090,
    },
    "hyderabad": {
        "city": "hyderabad",
        "lat": 17.3850,
        "lng": 78.4867,
    },
    "pune": {
        "city": "pune",
        "lat": 18.5204,
        "lng": 73.8567,
    },
    "chennai": {
        "city": "chennai",
        "lat": 13.0827,
        "lng": 80.2707,
    },
}

# Building type codes
BUILDING_TYPES = "AP,IH"  # AP=Apartment, IH=Independent House

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nobroker.in/",
}


class NoBrokerScraper(BaseScraper):
    """Scrape rental listings from NoBroker's internal API."""

    source_name = "nobroker"

    def scrape(self, params: SearchParams) -> list[RawPost]:
        """Scrape NoBroker listings for the given search params."""
        city_key = params.city.lower().strip()
        city_config = CITY_CONFIGS.get(city_key)

        if not city_config:
            print(f"  [!] NoBroker: City '{params.city}' not supported. Available: {list(CITY_CONFIGS.keys())}")
            return []

        # Build query parameters
        query_params = self._build_query(params, city_config)

        all_posts: list[RawPost] = []
        max_pages = min(5, (params.max_results // 25) + 1)  # NoBroker returns ~25 per page

        for page in range(1, max_pages + 1):
            query_params["pageNo"] = str(page)

            try:
                resp = httpx.get(
                    NOBROKER_API,
                    params=query_params,
                    headers=HEADERS,
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"  [!] NoBroker API error (page {page}): {e}")
                break

            # NoBroker wraps results in {"data": [...], "otherParams": {...}}
            listings_data = data.get("data", [])
            if not listings_data:
                break

            for item in listings_data:
                post = self._item_to_raw_post(item)
                if post:
                    all_posts.append(post)

            if len(all_posts) >= params.max_results:
                break

        # Fallback to mock data if API returned nothing
        if not all_posts:
            print("  [NoBroker] API returned no results, falling back to mock data")
            all_posts = self._load_mock_data()

        print(f"  [NoBroker] Found {len(all_posts)} listings for {params.city}")
        return all_posts[:params.max_results]

    def scrape_and_extract(self, params: SearchParams) -> tuple[list[RawPost], list[RentalListing]]:
        """
        Scrape NoBroker and return BOTH raw posts AND pre-extracted listings.
        This bypasses the generic _light_extract path, giving higher quality data
        since we use the structured API response directly.
        """
        city_key = params.city.lower().strip()
        city_config = CITY_CONFIGS.get(city_key)

        if not city_config:
            print(f"  [!] NoBroker: City '{params.city}' not supported.")
            return [], []

        query_params = self._build_query(params, city_config)
        all_posts: list[RawPost] = []
        all_listings: list[RentalListing] = []
        max_pages = min(5, (params.max_results // 25) + 1)

        for page in range(1, max_pages + 1):
            query_params["pageNo"] = str(page)
            try:
                resp = httpx.get(NOBROKER_API, params=query_params, headers=HEADERS, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"  [!] NoBroker API error (page {page}): {e}")
                break

            listings_data = data.get("data", [])
            if not listings_data:
                break

            for item in listings_data:
                post = self._item_to_raw_post(item)
                listing = self.extract_structured(item)
                if post:
                    all_posts.append(post)
                if listing:
                    all_listings.append(listing)

            if len(all_posts) >= params.max_results:
                break

        # Fallback to mock data
        if not all_posts:
            print("  [NoBroker] API returned no results, falling back to mock data")
            all_posts = self._load_mock_data()
            # Also extract from mock items
            mock_path = Path("sample_data/mock_nobroker.json")
            if mock_path.exists():
                with open(mock_path) as f:
                    items = json.load(f)
                for item in items:
                    listing = self.extract_structured(item)
                    if listing:
                        all_listings.append(listing)

        print(f"  [NoBroker] Found {len(all_posts)} posts, {len(all_listings)} pre-extracted listings")
        return all_posts[:params.max_results], all_listings[:params.max_results]

    def _load_mock_data(self) -> list[RawPost]:
        """Load mock data for development."""
        mock_path = Path("sample_data/mock_nobroker.json")
        if not mock_path.exists():
            return []
        with open(mock_path) as f:
            items = json.load(f)
        posts = []
        for item in items:
            post = self._item_to_raw_post(item)
            if post:
                posts.append(post)
        return posts

    def _build_query(self, params: SearchParams, city_config: dict) -> dict:
        """Build NoBroker API query parameters from SearchParams."""
        query: dict[str, str] = {
            "city": city_config["city"],
            "orderBy": "nbRank,desc",
            "radius": "2",
            "propertyType": "rent",
            "buildingType": BUILDING_TYPES,
            "sharedAccomodation": "0",
            "pageNo": "1",
        }

        # Budget range
        if params.budget_min or params.budget_max:
            rent_min = params.budget_min or 0
            rent_max = params.budget_max or 500000
            query["rent"] = f"{rent_min},{rent_max}"

        # BHK filter — NoBroker uses type like "2" for 2BHK
        if params.bedrooms:
            query["type"] = ",".join(str(b) for b in params.bedrooms)

        # Furnished filter
        if params.furnished:
            furnished_map = {
                "furnished": "FULLY",
                "semi-furnished": "SEMI",
                "unfurnished": "NOT",
            }
            nb_furnished = furnished_map.get(params.furnished, "")
            if nb_furnished:
                query["furnishing"] = nb_furnished

        # Listing type
        if params.listing_type == "full_flat":
            query["sharedAccomodation"] = "0"
        elif params.listing_type == "flatmate":
            query["sharedAccomodation"] = "1"

        return query

    @staticmethod
    def _build_nobroker_url(item: dict, listing_id: str) -> str:
        """Build a NoBroker property page URL from API fields."""
        city = (item.get("city") or "bangalore").lower().replace(" ", "-")
        locality = (item.get("localityName") or item.get("streetName") or "").lower().replace(" ", "-").replace(",", "")
        if locality:
            return f"https://www.nobroker.in/property/rent/{city}/{locality}/{listing_id}"
        return f"https://www.nobroker.in/property/rent/{city}/{listing_id}"

    def _item_to_raw_post(self, item: dict) -> RawPost | None:
        """Convert a NoBroker API listing to RawPost."""
        try:
            listing_id = item.get("id", "")
            if not listing_id:
                return None

            # Build descriptive text from NoBroker's structured fields
            title = item.get("title", "")
            desc = item.get("description", "")
            rent = item.get("rent", "")
            deposit = item.get("deposit", "")
            area_name = item.get("localityName", item.get("streetName", ""))
            city = item.get("city", "")
            bhk = item.get("type", "")
            furnishing = item.get("furnishing", "")
            building_type = item.get("buildingType", "")
            sqft = item.get("propertySize", "")
            floor = item.get("floor", "")
            total_floor = item.get("totalFloor", "")
            bathroom = item.get("bathroom", "")
            parking = item.get("parking", "")
            water_supply = item.get("waterSupply", "")
            gym = item.get("gym", "")

            # Construct readable text
            text_parts = []
            if title:
                text_parts.append(title)
            if bhk:
                text_parts.append(f"{bhk}BHK")
            if building_type:
                bt_map = {"AP": "Apartment", "IH": "Independent House", "IF": "Independent Floor", "VL": "Villa"}
                text_parts.append(bt_map.get(building_type, building_type))
            if area_name:
                text_parts.append(f"in {area_name}")
            if city:
                text_parts.append(f", {city}")
            if rent:
                text_parts.append(f"\nRent: ₹{rent}/month")
            if deposit:
                text_parts.append(f"Deposit: ₹{deposit}")
            if furnishing:
                furn_map = {"FULLY": "Fully Furnished", "SEMI": "Semi Furnished", "NOT": "Unfurnished"}
                text_parts.append(f"\n{furn_map.get(furnishing, furnishing)}")
            if sqft:
                text_parts.append(f"{sqft} sqft")
            if floor and total_floor:
                text_parts.append(f"Floor {floor}/{total_floor}")
            if desc:
                text_parts.append(f"\n{desc}")

            text = " ".join(text_parts)

            # Images
            photos = item.get("photos", [])
            image_urls = []
            for photo in photos:
                if isinstance(photo, dict):
                    url = photo.get("url", photo.get("imagesMap", {}).get("original", ""))
                    if url:
                        image_urls.append(url)
                elif isinstance(photo, str):
                    image_urls.append(photo)

            # Timestamp
            activated_on = item.get("activationDate", item.get("lastUpdateDate", ""))
            timestamp = ""
            if activated_on:
                try:
                    # NoBroker uses epoch milliseconds
                    if isinstance(activated_on, (int, float)):
                        timestamp = datetime.fromtimestamp(activated_on / 1000).isoformat()
                    else:
                        timestamp = str(activated_on)
                except (ValueError, OSError):
                    pass

            # Post URL
            property_url = self._build_nobroker_url(item, listing_id)

            return self._make_raw_post(
                post_id=listing_id,
                text=text,
                timestamp=timestamp,
                author=item.get("name", ""),
                image_urls=image_urls,
                post_url=property_url,
                source_listing_id=listing_id,
            )
        except Exception as e:
            print(f"  [!] NoBroker: Failed to parse listing: {e}")
            return None

    def extract_structured(self, item: dict) -> RentalListing | None:
        """
        Extract structured RentalListing directly from NoBroker API data.
        No Claude needed — the data is already structured.
        """
        try:
            listing_id = item.get("id", "")
            if not listing_id:
                return None

            # Furnishing mapping
            furn_map = {"FULLY": "furnished", "SEMI": "semi-furnished", "NOT": "unfurnished"}
            furnishing = furn_map.get(item.get("furnishing", ""), "")

            # Listing type
            shared = item.get("sharedAccomodation", False)
            listing_type = "flatmate" if shared else "full_flat"

            # Parking
            parking_val = item.get("parking", "")
            has_parking = None
            if parking_val and parking_val not in ("NONE", "0"):
                has_parking = True

            return RentalListing(
                raw_post_id=f"nobroker_{listing_id}",
                price=_safe_int(item.get("rent")),
                city=item.get("city", "").title(),
                area=item.get("localityName", item.get("streetName", "")),
                location_text=f"{item.get('localityName', '')}, {item.get('city', '').title()}",
                bedrooms=_safe_int(item.get("type")),
                bathrooms=_safe_int(item.get("bathroom")),
                furnished=furnishing,
                contact_phone=item.get("phone", ""),
                contact_name=item.get("name", ""),
                parking=has_parking,
                latitude=_safe_float(item.get("latitude")),
                longitude=_safe_float(item.get("longitude")),
                post_url=self._build_nobroker_url(item, listing_id),
                image_urls=[],  # Will be populated from photos
                post_type="supply",
                listing_type=listing_type,
                source="nobroker",
                source_listing_id=listing_id,
            )
        except Exception as e:
            print(f"  [!] NoBroker: Failed to extract listing: {e}")
            return None


def _safe_int(val) -> int | None:
    """Safely convert to int."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> float | None:
    """Safely convert to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
