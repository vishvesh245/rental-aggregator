import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(override=True)

BASE_DIR = Path(__file__).parent

APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")

# Database — Turso (cloud SQLite) if URL is set, else local file
TURSO_DB_URL = os.getenv("TURSO_DB_URL", "")
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "")
DB_PATH = BASE_DIR / "rentals.db"  # local fallback
MOCK_DATA_PATH = BASE_DIR / "sample_data" / "mock_posts.json"

# Claude model for extraction
CLAUDE_MODEL = "claude-sonnet-4-20250514"
