"""Extract structured rental details from raw posts using Claude API."""

from __future__ import annotations

import json

import anthropic

import config
from models import RawPost, RentalListing

EXTRACTION_TOOL = {
    "name": "save_rental_listing",
    "description": "Save a structured rental listing extracted from a Facebook post.",
    "input_schema": {
        "type": "object",
        "properties": {
            "is_rental_listing": {
                "type": "boolean",
                "description": "True if this post is someone offering a place for rent. False if it's a 'looking for' post, unrelated post, or commercial listing.",
            },
            "post_type": {
                "type": "string",
                "enum": ["supply", "demand", "other"],
                "description": "supply = offering a rental, demand = looking for a rental, other = unrelated",
            },
            "price": {
                "type": ["integer", "null"],
                "description": "Monthly rent in INR. Just the number, no symbols.",
            },
            "location_text": {
                "type": "string",
                "description": "Full location as mentioned in the post.",
            },
            "city": {
                "type": "string",
                "description": "City name (e.g., Bangalore, Mumbai, Hyderabad).",
            },
            "area": {
                "type": "string",
                "description": "Locality/area name (e.g., Koramangala, Andheri West, HSR Layout).",
            },
            "bedrooms": {
                "type": ["integer", "null"],
                "description": "Number of bedrooms. For 1RK/studio use 0.",
            },
            "bathrooms": {
                "type": ["integer", "null"],
                "description": "Number of bathrooms if mentioned.",
            },
            "furnished": {
                "type": "string",
                "enum": ["furnished", "semi-furnished", "unfurnished", ""],
                "description": "Furnishing status.",
            },
            "available_date": {
                "type": "string",
                "description": "When the property is available, in YYYY-MM-DD format if possible.",
            },
            "contact_phone": {
                "type": "string",
                "description": "Phone/WhatsApp number if mentioned.",
            },
            "contact_name": {
                "type": "string",
                "description": "Name of the contact person.",
            },
            "pet_friendly": {
                "type": ["boolean", "null"],
                "description": "True if pets are allowed, False if not, null if not mentioned.",
            },
            "parking": {
                "type": ["boolean", "null"],
                "description": "True if parking is available, null if not mentioned.",
            },
            "listing_type": {
                "type": "string",
                "enum": ["full_flat", "flatmate", "pg"],
                "description": "full_flat = entire apartment for rent, flatmate = room in shared flat / looking for flatmate replacement, pg = paying guest or hostel",
            },
        },
        "required": ["is_rental_listing", "post_type"],
    },
}

SYSTEM_PROMPT = """You are a rental listing data extractor for Indian cities.
Given a Facebook group post, extract structured rental information.

Rules:
- Posts offering a property = "supply"
- Posts looking for a property = "demand"
- Unrelated posts (packers/movers, questions, commercial) = "other"
- For Hindi/Hinglish posts, extract the same fields
- 1RK/studio = 0 bedrooms
- PG/hostel posts: extract the single room price as the price
- "L" or "lakh" means multiply by 100000 (e.g., 1.2L = 120000)
- "k" means multiply by 1000 (e.g., 25k = 25000)
- If a field isn't mentioned, leave it empty/null
- For roommate posts, treat as "supply" with the room price
- listing_type: "full_flat" if entire apartment, "flatmate" if room/flatmate replacement, "pg" if PG/hostel"""


def extract_rental_details(
    posts: list[RawPost],
    anthropic_api_key: str | None = None,
) -> list[RentalListing]:
    """
    Extract structured rental data from raw posts using Claude.

    If no API key is available, falls back to basic regex extraction.
    """
    api_key = anthropic_api_key or config.ANTHROPIC_API_KEY
    if not api_key:
        print("  [!] No ANTHROPIC_API_KEY set — using basic extraction fallback")
        return _basic_extract(posts)

    client = anthropic.Anthropic(api_key=api_key)
    listings = []

    for post in posts:
        try:
            listing = _extract_single(client, post)
            if listing:
                listings.append(listing)
        except Exception as e:
            print(f"  [!] Failed to extract post {post.post_id}: {e}")

    return listings


def _extract_single(client: anthropic.Anthropic, post: RawPost) -> RentalListing | None:
    """Extract details from a single post using Claude tool_use."""
    response = client.messages.create(
        model=config.CLAUDE_MODEL,
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        tools=[EXTRACTION_TOOL],
        tool_choice={"type": "tool", "name": "save_rental_listing"},
        messages=[
            {
                "role": "user",
                "content": f"Extract rental details from this Facebook post:\n\n{post.text}",
            }
        ],
    )

    # Get the tool use result
    for block in response.content:
        if block.type == "tool_use":
            data = block.input
            if not data.get("is_rental_listing") and data.get("post_type") == "other":
                return None

            return RentalListing(
                raw_post_id=post.post_id,
                price=data.get("price"),
                location_text=data.get("location_text", ""),
                city=data.get("city", ""),
                area=data.get("area", ""),
                bedrooms=data.get("bedrooms"),
                bathrooms=data.get("bathrooms"),
                furnished=data.get("furnished", ""),
                available_date=data.get("available_date", ""),
                contact_phone=data.get("contact_phone", ""),
                contact_name=data.get("contact_name", ""),
                pet_friendly=data.get("pet_friendly"),
                parking=data.get("parking"),
                post_url=post.post_url,
                image_urls=post.image_urls,
                post_type=data.get("post_type", "supply"),
                listing_type=data.get("listing_type", ""),
            )

    return None


def _basic_extract(posts: list[RawPost]) -> list[RentalListing]:
    """Fallback extraction using simple heuristics when no API key is available."""
    import re

    listings = []
    demand_keywords = ["looking for", "need", "wanted", "require", "searching"]

    for post in posts:
        text_lower = post.text.lower()

        # Skip non-rental posts
        if any(kw in text_lower for kw in ["packers", "movers", "electrician", "plumber"]):
            continue

        # Detect demand vs supply
        is_demand = any(kw in text_lower for kw in demand_keywords)
        post_type = "demand" if is_demand else "supply"

        # Extract phone first (so we can exclude it from price matching)
        phone_match = re.search(r'(\d{10})', post.text)
        contact_phone = phone_match.group(1) if phone_match else ""

        # Remove phone numbers from text before price extraction
        text_for_price = re.sub(r'\d{10}', '', post.text)

        # Extract price — handles ₹27,000 / 27K / Rs. 27000 / 27,000/month
        price = None
        # Try "25K" or "25k" pattern first
        price_match = re.search(r'(\d+[.,]?\d*)\s*(?:k|K)\b', text_for_price)
        if price_match:
            price = int(float(price_match.group(1).replace(",", "")) * 1000)
        else:
            # Try ₹27,000 or Rs. 27,000 or Rent: ₹27,000 (with commas)
            price_match = re.search(r'(?:rent\s*[:=]?\s*)?(?:Rs\.?|INR|₹)\s*([\d,]+)', text_for_price, re.IGNORECASE)
            if price_match:
                price = int(price_match.group(1).replace(",", ""))
            else:
                # Try "27,000/month" or "27000 per month"
                price_match = re.search(r'([\d,]{4,8})\s*(?:/\s*-?\s*(?:month|mo)|per\s*month|pm\b|rent|\/-)', text_for_price, re.IGNORECASE)
                if price_match:
                    price = int(price_match.group(1).replace(",", ""))
        # Sanity check: rent should be between 1000 and 5,00,000
        if price and (price < 1000 or price > 500000):
            price = None

        # Extract bedrooms
        bedrooms = None
        bhk_match = re.search(r'(\d)\s*(?:BHK|bhk)', post.text)
        if bhk_match:
            bedrooms = int(bhk_match.group(1))
        elif re.search(r'1\s*RK|studio', text_lower):
            bedrooms = 0

        # Furnished status
        furnished = ""
        if "semi" in text_lower and "furnish" in text_lower:
            furnished = "semi-furnished"
        elif "fully" in text_lower and "furnish" in text_lower:
            furnished = "furnished"
        elif "unfurnish" in text_lower:
            furnished = "unfurnished"
        elif "furnish" in text_lower:
            furnished = "furnished"

        # Extract location — match known Indian localities
        known_localities = {
            "koramangala": ("Koramangala", "Bangalore"),
            "indiranagar": ("Indiranagar", "Bangalore"),
            "hsr layout": ("HSR Layout", "Bangalore"),
            "whitefield": ("Whitefield", "Bangalore"),
            "marathahalli": ("Marathahalli", "Bangalore"),
            "electronic city": ("Electronic City", "Bangalore"),
            "btm layout": ("BTM Layout", "Bangalore"),
            "jayanagar": ("Jayanagar", "Bangalore"),
            "bellandur": ("Bellandur", "Bangalore"),
            "jp nagar": ("JP Nagar", "Bangalore"),
            "sarjapur": ("Sarjapur Road", "Bangalore"),
            "mg road": ("MG Road", "Bangalore"),
            "manyata": ("Manyata Tech Park", "Bangalore"),
            "andheri": ("Andheri West", "Mumbai"),
            "bandra": ("Bandra West", "Mumbai"),
            "powai": ("Powai", "Mumbai"),
            "jubilee hills": ("Jubilee Hills", "Hyderabad"),
        }

        area = ""
        city = ""
        location_text = ""
        for keyword, (loc_area, loc_city) in known_localities.items():
            if keyword in text_lower:
                area = loc_area
                city = loc_city
                location_text = f"{loc_area}, {loc_city}"
                break

        # Extract "L" / "lakh" prices (e.g., 1.2L = 120000)
        if price is None:
            lakh_match = re.search(r'(\d+\.?\d*)\s*(?:L|lakh)', post.text, re.IGNORECASE)
            if lakh_match:
                price = int(float(lakh_match.group(1)) * 100000)

        # Fallback: any 5-6 digit number likely a rent (use phone-stripped text)
        if price is None:
            rent_match = re.search(r'\b(\d{5,6})\b', text_for_price)
            if rent_match:
                price = int(rent_match.group(1))

        # Pet friendly
        pet_friendly = None
        if "pet" in text_lower and ("friendly" in text_lower or "allowed" in text_lower or "ok" in text_lower):
            pet_friendly = True
        elif "no pet" in text_lower:
            pet_friendly = False

        # Parking
        parking = True if "parking" in text_lower else None

        listings.append(RentalListing(
            raw_post_id=post.post_id,
            price=price,
            location_text=location_text,
            city=city,
            area=area,
            bedrooms=bedrooms,
            contact_phone=contact_phone,
            furnished=furnished,
            pet_friendly=pet_friendly,
            parking=parking,
            post_url=post.post_url,
            image_urls=post.image_urls,
            post_type=post_type,
        ))

    return listings
