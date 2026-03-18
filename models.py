from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class RawPost:
    post_id: str
    text: str
    timestamp: str
    author: str = ""
    image_urls: list[str] = field(default_factory=list)
    post_url: str = ""
    group_url: str = ""
    source: str = "facebook"              # "facebook", "nobroker", "99acres", "magicbricks", "housing"
    source_listing_id: str = ""           # Original listing ID on the source platform


@dataclass
class RentalListing:
    raw_post_id: str
    price: int | None = None
    price_currency: str = "INR"
    location_text: str = ""
    city: str = ""
    area: str = ""
    bedrooms: int | None = None
    bathrooms: int | None = None
    furnished: str = ""          # "furnished", "semi-furnished", "unfurnished", ""
    available_date: str = ""
    contact_phone: str = ""
    contact_name: str = ""
    pet_friendly: bool | None = None
    parking: bool | None = None
    latitude: float | None = None
    longitude: float | None = None
    post_url: str = ""
    image_urls: list[str] = field(default_factory=list)
    post_type: str = "supply"    # "supply" (listing) or "demand" (looking for)
    listing_type: str = ""       # "full_flat", "flatmate", "pg", ""
    extracted_at: str = field(default_factory=lambda: datetime.now().isoformat())
    source: str = "facebook"     # "facebook", "nobroker", "99acres", "magicbricks", "housing"
    source_listing_id: str = ""  # Original listing ID on the source platform
