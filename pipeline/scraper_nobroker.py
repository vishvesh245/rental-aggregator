"""NoBroker scraper — hits their internal API directly.

NoBroker's filter API now requires a Google Places placeId for the locality.
We resolve this via the Google Geocoding API using the project's GOOGLE_MAPS_API_KEY.
"""

from __future__ import annotations

import json
import hashlib
from datetime import datetime
from pathlib import Path

import httpx

import config
from models import RawPost, RentalListing
from pipeline.base_scraper import BaseScraper, SearchParams

# Cache place IDs to avoid repeat geocoding calls
_PLACE_ID_CACHE: dict[str, str] = {}

# Hardcoded place IDs for common Bangalore areas (avoids API call)
KNOWN_PLACE_IDS: dict[str, str] = {
    "bangalore": "ChIJbU60yXAWrjsR4E9-UejD3_g",
    "koramangala": "ChIJLfyY2E4UrjsRVq4AjI7zgRY",
    "hsr layout": "ChIJ0RIoNXAUrjsRc_OZJmYJnzQ",
    "indiranagar": "ChIJgzHBmforUjsRiXnRFyxXo5c",
    "whitefield": "ChIJj4KuNxMUrjsR6HJh8XLkQlQ",
    "marathahalli": "ChIJlXd4gYoUrjsR6rXKgTd0zFQ",
    "electronic city": "ChIJR-3znmMTrjsRiTb63sQWFpc",
    "btm layout": "ChIJH3WT-FMUrjsR3wchWLtJBCk",
    "jp nagar": "ChIJJ7-BNIMUrjsR4b3fEKkTmqQ",
    "bellandur": "ChIJuxnTvccUrjsR8PXzJz5HPVM",
    "sarjapur road": "ChIJKQQpzboUrjsR8V3tLX3MJeU",
    "hebbal": "ChIJ7xqP3csWrjsR0jJeQkT_K4c",
    "bannerghatta road": "ChIJKRxKRXsUrjsRTAb1zK1X5V8",
    "jayanagar": "ChIJZVsPGnQUrjsREDnSxDRkHQA",
}


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
        max_pages = min(10, (params.max_results // 25) + 1)  # NoBroker returns ~25 per page

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

            print(f"  [NoBroker] Page {page}: {len(all_posts)} total so far")
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
        max_pages = min(10, (params.max_results // 25) + 1)

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

    def _get_place_id(self, location: str, city: str) -> str | None:
        """Resolve a location string to a Google Places placeId."""
        key = f"{location.lower()},{city.lower()}"

        # Check hardcoded list first
        place_id = KNOWN_PLACE_IDS.get(location.lower()) or KNOWN_PLACE_IDS.get(city.lower())
        if place_id:
            return place_id

        # Check cache
        if key in _PLACE_ID_CACHE:
            return _PLACE_ID_CACHE[key]

        # Fall back to Google Geocoding API
        if not config.GOOGLE_MAPS_API_KEY:
            return None

        try:
            resp = httpx.get(
                "https://maps.googleapis.com/maps/api/geocode/json",
                params={"address": f"{location}, {city}, India", "key": config.GOOGLE_MAPS_API_KEY},
                timeout=10,
            )
            results = resp.json().get("results", [])
            if results:
                pid = results[0]["place_id"]
                _PLACE_ID_CACHE[key] = pid
                return pid
        except Exception as e:
            print(f"  [!] NoBroker: Failed to geocode '{location}': {e}")

        return None

    def _build_query(self, params: SearchParams, city_config: dict) -> dict:
        """Build NoBroker API query parameters from SearchParams."""
        # Resolve location to Google placeId (required by NoBroker API)
        location = (params.areas[0] if params.areas else params.city)
        place_id = self._get_place_id(location, params.city)

        query: dict[str, str] = {
            "city": city_config["city"],
            "orderBy": "nbRank,desc",
            "radius": "3",
            "propertyType": "rent",
            "buildingType": BUILDING_TYPES,
            "sharedAccomodation": "0",
            "pageNo": "1",
        }

        if place_id:
            query["placeId"] = place_id
            print(f"  [NoBroker] Using placeId={place_id} for '{location}'")

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
        """Get the canonical NoBroker property page URL from API response."""
        # Prefer the API-provided canonical URL
        detail_url = item.get("detailUrl", "")
        if detail_url:
            if detail_url.startswith("/"):
                return f"https://www.nobroker.in{detail_url}"
            return detail_url
        # Fallback: use shortUrl if available
        short_url = item.get("shortUrl", "")
        if short_url:
            return short_url
        # Last resort: construct manually
        city = (item.get("city") or "bangalore").lower().replace(" ", "-")
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
            area_name = (
                item.get("locality")
                or item.get("nbLocality")
                or item.get("localityName")
                or item.get("streetName")
                or ""
            )
            city = item.get("city", "")
            bhk_raw = item.get("typeDesc") or item.get("type", "")
            # Normalize "BHK2" → "2 BHK", pass through "2 BHK" as-is
            import re as _re
            bhk_num = _re.search(r'\d+', bhk_raw)
            bhk = f"{bhk_num.group()} BHK" if bhk_num else bhk_raw
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
                text_parts.append(bhk)
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
                furn_map = {
                    "FULLY": "Fully Furnished", "FULLY_FURNISHED": "Fully Furnished",
                    "SEMI": "Semi Furnished", "SEMI_FURNISHED": "Semi Furnished",
                    "NOT": "Unfurnished", "NOT_FURNISHED": "Unfurnished", "UNFURNISHED": "Unfurnished",
                }
                text_parts.append(f"\n{furn_map.get(furnishing.upper(), furnishing)}")
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
            for photo in photos[:5]:  # Limit to 5 photos
                if isinstance(photo, dict):
                    imgs_map = photo.get("imagesMap", {})
                    filename = imgs_map.get("medium") or imgs_map.get("large") or imgs_map.get("original", "")
                    if filename and not filename.startswith("http"):
                        # Build CDN URL: https://images.nobroker.in/images/{listingId}/{filename}
                        filename = f"https://images.nobroker.in/images/{listing_id}/{filename}"
                    if filename:
                        image_urls.append(filename)
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

            # Furnishing mapping (API may return FULLY/SEMI/NOT or FULLY_FURNISHED/SEMI_FURNISHED/NOT_FURNISHED)
            furn_raw = item.get("furnishing", "") or ""
            furn_map = {
                "FULLY": "furnished", "FULLY_FURNISHED": "furnished",
                "SEMI": "semi-furnished", "SEMI_FURNISHED": "semi-furnished",
                "NOT": "unfurnished", "NOT_FURNISHED": "unfurnished",
                "UNFURNISHED": "unfurnished",
            }
            furnishing = furn_map.get(furn_raw.upper(), "")

            # Listing type
            shared = item.get("sharedAccomodation", False)
            listing_type = "flatmate" if shared else "full_flat"

            # Parking
            parking_val = item.get("parking", "")
            has_parking = None
            if parking_val and parking_val not in ("NONE", "0"):
                has_parking = True

            # BHK: API returns "BHK2", "BHK1", "BHK3" etc. — extract the number
            bhk_raw = item.get("type", "") or item.get("typeDesc", "")
            bedrooms = _parse_bhk(bhk_raw)

            # Area: API uses "locality" or "nbLocality" (not "localityName")
            area = (
                item.get("locality")
                or item.get("nbLocality")
                or item.get("localityName")
                or item.get("streetName")
                or ""
            )

            return RentalListing(
                raw_post_id=f"nobroker_{listing_id}",
                price=_safe_int(item.get("rent")),
                city=item.get("city", "").title(),
                area=area,
                location_text=f"{area}, {item.get('city', '').title()}",
                bedrooms=bedrooms,
                bathrooms=_safe_int(item.get("bathroom")),
                furnished=furnishing,
                contact_phone=item.get("phone", ""),
                contact_name=item.get("name", ""),
                parking=has_parking,
                latitude=_safe_float(item.get("latitude")),
                longitude=_safe_float(item.get("longitude")),
                post_url=self._build_nobroker_url(item, listing_id),
                image_urls=_extract_photo_urls(item, listing_id),
                post_type="supply",
                listing_type=listing_type,
                source="nobroker",
                source_listing_id=listing_id,
            )
        except Exception as e:
            print(f"  [!] NoBroker: Failed to extract listing: {e}")
            return None


def _parse_bhk(val) -> int | None:
    """Parse BHK count from strings like 'BHK2', '2 BHK', '2', or int 2."""
    if val is None:
        return None
    if isinstance(val, int):
        return val
    import re
    m = re.search(r'\d+', str(val))
    return int(m.group()) if m else None


def _extract_photo_urls(item: dict, listing_id: str) -> list[str]:
    """Extract up to 5 CDN photo URLs from a NoBroker API item."""
    urls = []
    for photo in item.get("photos", [])[:5]:
        if isinstance(photo, dict):
            imgs_map = photo.get("imagesMap", {})
            filename = imgs_map.get("medium") or imgs_map.get("large") or imgs_map.get("original", "")
            if filename and not filename.startswith("http"):
                filename = f"https://images.nobroker.in/images/{listing_id}/{filename}"
            if filename:
                urls.append(filename)
    return urls


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
