"""Geocode rental listings using Google Maps API."""

from __future__ import annotations

import config
from models import RentalListing


# In-memory cache to avoid duplicate API calls for the same location
_geocode_cache: dict[str, tuple[float, float] | None] = {}


def geocode_listings(
    listings: list[RentalListing],
    default_city: str = "",
) -> list[RentalListing]:
    """
    Add lat/lng coordinates to listings using Google Maps Geocoding API.

    Falls back to mock coordinates if no API key is set.
    """
    if not config.GOOGLE_MAPS_API_KEY:
        print("  [!] No GOOGLE_MAPS_API_KEY set — using mock coordinates")
        return _mock_geocode(listings)

    import googlemaps
    gmaps = googlemaps.Client(key=config.GOOGLE_MAPS_API_KEY)

    for listing in listings:
        location = _build_location_query(listing, default_city)
        if not location:
            continue

        if location in _geocode_cache:
            coords = _geocode_cache[location]
        else:
            coords = _geocode_single(gmaps, location)
            _geocode_cache[location] = coords

        if coords:
            listing.latitude, listing.longitude = coords

    return listings


def _build_location_query(listing: RentalListing, default_city: str) -> str:
    """Build a geocoding query string from listing location fields."""
    parts = []
    if listing.area:
        parts.append(listing.area)
    elif listing.location_text:
        parts.append(listing.location_text)

    city = listing.city or default_city
    if city and city.lower() not in " ".join(parts).lower():
        parts.append(city)

    if parts and "india" not in " ".join(parts).lower():
        parts.append("India")

    return ", ".join(parts)


def _geocode_single(gmaps, query: str) -> tuple[float, float] | None:
    """Geocode a single location string."""
    try:
        results = gmaps.geocode(query)
        if results:
            loc = results[0]["geometry"]["location"]
            return (loc["lat"], loc["lng"])
    except Exception as e:
        print(f"  [!] Geocoding failed for '{query}': {e}")
    return None


# Approximate coordinates for common Indian localities (for demo without API key)
_MOCK_COORDS: dict[str, tuple[float, float]] = {
    "koramangala": (12.9352, 77.6245),
    "indiranagar": (12.9784, 77.6408),
    "hsr layout": (12.9116, 77.6474),
    "whitefield": (12.9698, 77.7500),
    "marathahalli": (12.9591, 77.7009),
    "electronic city": (12.8399, 77.6770),
    "btm layout": (12.9166, 77.6101),
    "jayanagar": (12.9308, 77.5838),
    "bellandur": (12.9260, 77.6762),
    "jp nagar": (12.9063, 77.5857),
    "sarjapur road": (12.9107, 77.6872),
    "mg road": (12.9756, 77.6066),
    "manyata tech park": (13.0472, 77.6217),
    "andheri west": (19.1364, 72.8296),
    "bandra west": (19.0596, 72.8295),
    "powai": (19.1176, 72.9060),
    "jubilee hills": (17.4325, 78.4073),
}


def _mock_geocode(listings: list[RentalListing]) -> list[RentalListing]:
    """Assign approximate coordinates based on known locality names."""
    for listing in listings:
        search_text = f"{listing.area} {listing.location_text}".lower()
        for locality, coords in _MOCK_COORDS.items():
            if locality in search_text:
                listing.latitude, listing.longitude = coords
                break

    return listings
