"""Generate broker outreach messages for WhatsApp/SMS."""

from __future__ import annotations

import anthropic
import config


def generate_broker_message(listing: dict, user_name: str = "") -> str:
    """
    Generate a polite WhatsApp/SMS message for broker outreach.

    If Claude API is available, uses it to polish the message.
    Otherwise falls back to a template.
    """
    # Build base message from template
    message = _template_message(listing, user_name)

    # Optionally polish with Claude (if API key available)
    if config.ANTHROPIC_API_KEY:
        try:
            message = _polish_with_claude(message, listing)
        except Exception as e:
            print(f"  [!] Message generation Claude error: {e}")
            # Fall back to template

    return message


def _template_message(listing: dict, user_name: str = "") -> str:
    """Generate a basic template message."""
    name_part = f"This is {user_name}. " if user_name else ""
    contact = listing.get("contact_name", "")
    greeting = f"Hi {contact}," if contact else "Hi,"

    bhk = f"{listing.get('bedrooms', '')}BHK " if listing.get('bedrooms') else ""
    area = listing.get("area", "")
    city = listing.get("city", "")
    location = f"in {area}, {city}" if area and city else f"in {area or city}" if (area or city) else ""
    price = f"₹{listing['price']:,}/month" if listing.get("price") else ""

    source = listing.get("source", "")
    source_name = {
        "facebook": "Facebook",
        "nobroker": "NoBroker",
        "99acres": "99acres",
        "magicbricks": "MagicBricks",
        "housing": "Housing.com",
    }.get(source, source)

    msg = f"""{greeting}

{name_part}I came across your {bhk}property listing {location}{f' (listed at {price})' if price else ''}{f' on {source_name}' if source_name else ''}.

Is this property still available? I'd love to schedule a visit at your earliest convenience.

Looking forward to hearing from you.
Thank you!"""

    return msg.strip()


def _polish_with_claude(template: str, listing: dict) -> str:
    """Use Claude to polish the message — keep it concise and natural."""
    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)

    response = client.messages.create(
        model=config.CLAUDE_MODEL,
        max_tokens=300,
        system="You polish WhatsApp messages for property inquiries. Keep them short (under 100 words), friendly, and natural. Don't be overly formal. Return ONLY the message text, nothing else.",
        messages=[
            {
                "role": "user",
                "content": f"Polish this property inquiry message. Keep the key details but make it sound natural for WhatsApp:\n\n{template}",
            }
        ],
    )

    return response.content[0].text.strip()
