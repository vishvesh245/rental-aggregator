"""Deduplication — detect and group same listings across sources."""

from __future__ import annotations

import uuid
from collections import defaultdict

from models import RentalListing


def deduplicate_listings(listings: list[RentalListing]) -> list[RentalListing]:
    """
    Detect duplicate listings across sources and mark them.

    Uses fuzzy matching on (city, area, bedrooms, price within 10%).
    Groups duplicates together and marks one as canonical.

    Returns the same list with duplicate_group_id and is_canonical set
    (via _dedup_meta attribute).
    """
    if not listings:
        return listings

    # Build groups by fuzzy key
    groups: dict[str, list[int]] = defaultdict(list)

    for i, listing in enumerate(listings):
        key = _fuzzy_key(listing)
        if key:
            groups[key].append(i)

    # Assign group IDs
    dedup_meta: dict[int, dict] = {}  # index -> {group_id, is_canonical}

    for key, indices in groups.items():
        if len(indices) <= 1:
            # Unique listing — no dedup needed
            dedup_meta[indices[0]] = {"group_id": "", "is_canonical": True, "also_on": []}
            continue

        # Multiple listings match — group them
        group_id = uuid.uuid4().hex[:12]

        # Pick canonical: prefer structured sources over social, then most recent
        source_priority = {"nobroker": 1, "99acres": 2, "magicbricks": 3, "housing": 4, "facebook": 5}
        sorted_indices = sorted(
            indices,
            key=lambda i: source_priority.get(listings[i].source, 99),
        )

        for rank, idx in enumerate(sorted_indices):
            other_sources = [
                listings[j].source
                for j in sorted_indices
                if j != idx
            ]
            dedup_meta[idx] = {
                "group_id": group_id,
                "is_canonical": rank == 0,
                "also_on": other_sources,
            }

    # Attach metadata to listings
    for i, listing in enumerate(listings):
        meta = dedup_meta.get(i, {"group_id": "", "is_canonical": True, "also_on": []})
        listing._dedup_meta = meta  # type: ignore

    # Count stats
    total_groups = len([g for g in groups.values() if len(g) > 1])
    total_dupes = sum(len(g) - 1 for g in groups.values() if len(g) > 1)
    if total_groups > 0:
        print(f"  [Dedup] Found {total_groups} duplicate groups ({total_dupes} duplicates)")

    return listings


def _fuzzy_key(listing: RentalListing) -> str:
    """
    Create a fuzzy matching key from listing fields.
    Matches on: city (normalized) + area (normalized) + bedrooms + price bucket.
    """
    if not listing.city and not listing.area:
        return ""

    city = listing.city.lower().strip() if listing.city else "unknown"
    area = _normalize_area(listing.area) if listing.area else "unknown"
    bedrooms = str(listing.bedrooms) if listing.bedrooms is not None else "x"

    # Price bucket: round to nearest 2000
    if listing.price:
        price_bucket = str((listing.price // 2000) * 2000)
    else:
        price_bucket = "0"

    return f"{city}|{area}|{bedrooms}|{price_bucket}"


def _normalize_area(area: str) -> str:
    """Normalize area names for comparison."""
    area = area.lower().strip()

    # Common normalizations
    replacements = {
        "hsr": "hsr layout",
        "btm": "btm layout",
        "jp nagar": "jp nagar",
        "j.p. nagar": "jp nagar",
        "electronic city": "electronic city",
        "e-city": "electronic city",
        "ecity": "electronic city",
        "koramangala": "koramangala",
        "kormangala": "koramangala",
        "indiranagar": "indiranagar",
        "indira nagar": "indiranagar",
        "whitefield": "whitefield",
        "white field": "whitefield",
        "marathahalli": "marathahalli",
        "marthahalli": "marathahalli",
        "sarjapur": "sarjapur road",
        "sarjapur rd": "sarjapur road",
        "bellandur": "bellandur",
        "bellundur": "bellandur",
    }

    for pattern, normalized in replacements.items():
        if pattern in area:
            return normalized

    # Remove common suffixes (word-boundary only to avoid corrupting names like "indiranagar")
    import re
    for suffix in ["layout", "road", "rd", "nagar", "block", "sector", "phase", "stage"]:
        area = re.sub(r'\b' + suffix + r'\b', '', area).strip()

    return area.strip()


def get_canonical_listings(listings: list[dict]) -> list[dict]:
    """
    Filter a list of listing dicts to only canonical ones.
    Also adds 'also_on' field showing other sources.
    """
    # Group by dedup key
    groups: dict[str, list[dict]] = defaultdict(list)

    for listing in listings:
        key = _fuzzy_key_from_dict(listing)
        if key:
            groups[key].append(listing)
        else:
            groups[f"unique_{id(listing)}"] = [listing]

    canonical = []
    for key, group in groups.items():
        if len(group) == 1:
            group[0]["also_on"] = []
            canonical.append(group[0])
        else:
            # Pick best by source priority
            source_priority = {"nobroker": 1, "99acres": 2, "magicbricks": 3, "housing": 4, "facebook": 5}
            sorted_group = sorted(
                group,
                key=lambda l: source_priority.get(l.get("source", ""), 99),
            )
            best = sorted_group[0]
            best["also_on"] = [l.get("source", "") for l in sorted_group[1:]]
            canonical.append(best)

    return canonical


def _fuzzy_key_from_dict(listing: dict) -> str:
    """Create fuzzy key from a dict listing (from DB query)."""
    city = (listing.get("city") or "").lower().strip()
    area = _normalize_area(listing.get("area") or "")
    bedrooms = str(listing.get("bedrooms", "x"))
    price = listing.get("price")
    price_bucket = str((price // 2000) * 2000) if price else "0"
    return f"{city}|{area}|{bedrooms}|{price_bucket}"
