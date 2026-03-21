"""SQLite storage layer for rental listings.

Uses Turso (cloud SQLite via libsql_client) when TURSO_DB_URL is set,
otherwise falls back to local SQLite file.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict

import config
from models import RawPost, RentalListing

# ─── Turso / local SQLite abstraction ───
_use_turso = bool(config.TURSO_DB_URL)
_turso_url = ""

if _use_turso:
    try:
        import libsql_client as _libsql_mod
        _turso_url = config.TURSO_DB_URL.replace("libsql://", "https://")
        print(f"  [DB] Using Turso: {config.TURSO_DB_URL[:50]}...")
    except ImportError:
        print("  [DB] libsql_client not installed, falling back to local SQLite")
        _use_turso = False


class _TursoConn:
    """Thin wrapper around libsql_client to match sqlite3.Connection API."""

    def __init__(self):
        # Fresh client per connection — avoids async event loop deadlocks
        self._client = _libsql_mod.create_client_sync(
            url=_turso_url,
            auth_token=config.TURSO_AUTH_TOKEN,
        )

    def execute(self, sql: str, params=None) -> "_TursoResult":
        args = list(params) if params else []
        result = self._client.execute(sql, args)
        return _TursoResult(result)

    def executescript(self, sql: str):
        """Execute multiple statements separated by semicolons."""
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        for stmt in statements:
            self._client.execute(stmt)

    def commit(self):
        pass  # Turso auto-commits each statement

    def close(self):
        self._client.close()


class _RowProxy:
    """Dict-like object that also supports positional indexing like sqlite3.Row."""

    def __init__(self, columns, values):
        self._columns = columns
        self._values = values
        self._map = dict(zip(columns, values))

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return self._map[key]

    def get(self, key, default=None):
        return self._map.get(key, default)

    def keys(self):
        return self._columns

    def values(self):
        return self._values

    def items(self):
        return self._map.items()

    def __iter__(self):
        return iter(self._columns)

    def __contains__(self, key):
        return key in self._map


class _TursoResult:
    """Wraps libsql_client result to provide sqlite3-compatible access."""

    def __init__(self, result):
        self._result = result
        self._columns = result.columns if result.columns else ()
        self._rows = result.rows if result.rows else []

    def fetchall(self) -> list[_RowProxy]:
        return [_RowProxy(self._columns, row) for row in self._rows]

    def fetchone(self) -> _RowProxy | None:
        if self._rows:
            return _RowProxy(self._columns, self._rows[0])
        return None

    @property
    def rowcount(self) -> int:
        return len(self._rows)


def _get_conn():
    if _use_turso:
        return _TursoConn()
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    """Create tables if they don't exist."""
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS raw_posts (
            post_id TEXT PRIMARY KEY,
            text TEXT NOT NULL,
            timestamp TEXT,
            author TEXT,
            image_urls TEXT,
            post_url TEXT,
            group_url TEXT,
            source TEXT DEFAULT 'facebook',
            source_listing_id TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS rental_listings (
            raw_post_id TEXT PRIMARY KEY,
            price INTEGER,
            price_currency TEXT DEFAULT 'INR',
            location_text TEXT,
            city TEXT,
            area TEXT,
            bedrooms INTEGER,
            bathrooms INTEGER,
            furnished TEXT,
            available_date TEXT,
            contact_phone TEXT,
            contact_name TEXT,
            pet_friendly INTEGER,
            parking INTEGER,
            latitude REAL,
            longitude REAL,
            post_url TEXT,
            image_urls TEXT,
            post_type TEXT DEFAULT 'supply',
            listing_type TEXT DEFAULT '',
            extracted_at TEXT,
            source TEXT DEFAULT 'facebook',
            source_listing_id TEXT DEFAULT ''
        );

        CREATE INDEX IF NOT EXISTS idx_listings_city ON rental_listings(city);
        CREATE INDEX IF NOT EXISTS idx_listings_price ON rental_listings(price);
        CREATE INDEX IF NOT EXISTS idx_listings_area ON rental_listings(area);
        CREATE INDEX IF NOT EXISTS idx_listings_type ON rental_listings(post_type);
        CREATE INDEX IF NOT EXISTS idx_listings_source ON rental_listings(source);
        CREATE INDEX IF NOT EXISTS idx_listings_extracted_at ON rental_listings(extracted_at);

        CREATE TABLE IF NOT EXISTS user_preferences (
            id TEXT PRIMARY KEY,
            city TEXT NOT NULL DEFAULT 'Bangalore',
            areas TEXT DEFAULT '[]',
            budget_min INTEGER,
            budget_max INTEGER,
            bedrooms TEXT DEFAULT '[2, 3]',
            furnished TEXT DEFAULT '',
            listing_type TEXT DEFAULT '',
            is_active INTEGER DEFAULT 1,
            created_at TEXT,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS scrape_runs (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            status TEXT DEFAULT 'running',
            listings_found INTEGER DEFAULT 0,
            listings_new INTEGER DEFAULT 0,
            error_message TEXT
        );
    """)

    # Clean up MagicBricks data — source removed (mostly sale listings)
    conn.execute("DELETE FROM rental_listings WHERE source = 'magicbricks'")
    conn.execute("DELETE FROM raw_posts WHERE source = 'magicbricks'")
    conn.commit()

    conn.close()


def save_raw_posts(posts: list[RawPost]) -> int:
    """Save raw posts to the database. Returns number of new posts saved."""
    conn = _get_conn()
    saved = 0
    for post in posts:
        try:
            conn.execute(
                """INSERT OR REPLACE INTO raw_posts
                   (post_id, text, timestamp, author, image_urls, post_url, group_url, source, source_listing_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    post.post_id,
                    post.text,
                    post.timestamp,
                    post.author,
                    json.dumps(post.image_urls),
                    post.post_url,
                    post.group_url,
                    getattr(post, 'source', 'facebook'),
                    getattr(post, 'source_listing_id', ''),
                ),
            )
            saved += 1
        except sqlite3.Error as e:
            print(f"  [!] Failed to save post {post.post_id}: {e}")
    conn.commit()
    conn.close()
    return saved


def save_listings(listings: list[RentalListing]) -> int:
    """Save rental listings to the database. Returns number saved."""
    conn = _get_conn()
    saved = 0
    for listing in listings:
        try:
            conn.execute(
                """INSERT OR REPLACE INTO rental_listings
                   (raw_post_id, price, price_currency, location_text, city, area,
                    bedrooms, bathrooms, furnished, available_date, contact_phone,
                    contact_name, pet_friendly, parking, latitude, longitude,
                    post_url, image_urls, post_type, listing_type, extracted_at,
                    source, source_listing_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    listing.raw_post_id,
                    listing.price,
                    listing.price_currency,
                    listing.location_text,
                    listing.city,
                    listing.area,
                    listing.bedrooms,
                    listing.bathrooms,
                    listing.furnished,
                    listing.available_date,
                    listing.contact_phone,
                    listing.contact_name,
                    1 if listing.pet_friendly else (0 if listing.pet_friendly is False else None),
                    1 if listing.parking else (0 if listing.parking is False else None),
                    listing.latitude,
                    listing.longitude,
                    listing.post_url,
                    json.dumps(listing.image_urls),
                    listing.post_type,
                    listing.listing_type,
                    listing.extracted_at,
                    getattr(listing, 'source', 'facebook'),
                    getattr(listing, 'source_listing_id', ''),
                ),
            )
            saved += 1
        except sqlite3.Error as e:
            print(f"  [!] Failed to save listing {listing.raw_post_id}: {e}")
    conn.commit()
    conn.close()
    return saved


def get_listings_summary() -> dict:
    """
    Return a summary of the most recent cached listings for the landing page hero card.
    Includes: count, last scraped time, city/bedrooms/price breakdown.
    """
    conn = _get_conn()
    rent_cap = "AND (price IS NULL OR price <= 150000)"

    count = conn.execute(
        f"SELECT COUNT(*) FROM rental_listings WHERE post_type = 'supply' {rent_cap}"
    ).fetchone()[0]

    if count == 0:
        conn.close()
        return {"count": 0}

    # Most recent extraction time across all supply listings
    last_row = conn.execute(
        f"SELECT extracted_at FROM rental_listings WHERE post_type = 'supply' {rent_cap} ORDER BY extracted_at DESC LIMIT 1"
    ).fetchone()
    last_scraped_at = last_row[0] if last_row else None

    # Top city
    city_row = conn.execute(
        f"""SELECT city, COUNT(*) as n FROM rental_listings
            WHERE post_type='supply' AND city != '' {rent_cap}
            GROUP BY LOWER(city) ORDER BY n DESC LIMIT 1"""
    ).fetchone()
    top_city = city_row["city"] if city_row else ""

    # Top bedrooms
    bhk_row = conn.execute(
        f"""SELECT bedrooms, COUNT(*) as n FROM rental_listings
            WHERE post_type='supply' AND bedrooms IS NOT NULL {rent_cap}
            GROUP BY bedrooms ORDER BY n DESC LIMIT 1"""
    ).fetchone()
    top_bhk = bhk_row["bedrooms"] if bhk_row else None

    # Price range (p10 to p90 to avoid outliers)
    price_rows = conn.execute(
        f"SELECT price FROM rental_listings WHERE post_type='supply' AND price IS NOT NULL {rent_cap} ORDER BY price"
    ).fetchall()
    prices = [r[0] for r in price_rows]
    price_min = None
    price_max = None
    if prices:
        p10 = prices[max(0, len(prices) // 10)]
        p90 = prices[min(len(prices) - 1, (len(prices) * 9) // 10)]
        price_min = p10
        price_max = p90

    # Source breakdown
    source_rows = conn.execute(
        f"""SELECT source, COUNT(*) as n FROM rental_listings
            WHERE post_type='supply' {rent_cap}
            GROUP BY source ORDER BY n DESC"""
    ).fetchall()
    by_source = {r["source"]: r["n"] for r in source_rows}

    conn.close()
    return {
        "count": count,
        "last_scraped_at": last_scraped_at,
        "top_city": top_city,
        "top_bhk": top_bhk,
        "price_min": price_min,
        "price_max": price_max,
        "by_source": by_source,
    }


def get_listings(
    city: str = "",
    area: str = "",
    price_min: int | None = None,
    price_max: int | None = None,
    bedrooms: int | None = None,
    post_type: str = "supply",
    listing_type: str = "",
    furnished: str = "",
    source: str = "",
    days: int | None = None,
) -> list[dict]:
    """Query listings with optional filters."""
    conn = _get_conn()
    query = """SELECT l.*, r.text as raw_text, r.timestamp as post_date, r.author
               FROM rental_listings l
               LEFT JOIN raw_posts r ON l.raw_post_id = r.post_id
               WHERE 1=1"""
    params: list = []

    if post_type:
        query += " AND l.post_type = ?"
        params.append(post_type)
    if city:
        query += " AND LOWER(l.city) LIKE ?"
        params.append(f"%{city.lower()}%")
    if area:
        query += " AND LOWER(l.area) LIKE ?"
        params.append(f"%{area.lower()}%")
    if price_min is not None:
        query += " AND l.price >= ?"
        params.append(price_min)
    if price_max is not None:
        query += " AND l.price <= ?"
        params.append(price_max)
    if bedrooms is not None:
        if bedrooms >= 3:
            query += " AND l.bedrooms >= ?"
        else:
            query += " AND l.bedrooms = ?"
        params.append(bedrooms)
    if listing_type:
        query += " AND l.listing_type = ?"
        params.append(listing_type)
    if furnished:
        query += " AND LOWER(l.furnished) = ?"
        params.append(furnished.lower())
    if source:
        sources = [s.strip() for s in source.split(",") if s.strip()]
        if len(sources) == 1:
            query += " AND l.source = ?"
            params.append(sources[0])
        elif sources:
            placeholders = ",".join("?" for _ in sources)
            query += f" AND l.source IN ({placeholders})"
            params.extend(sources)

    if days is not None:
        query += f" AND l.extracted_at >= datetime('now', '-{int(days)} days')"

    # Exclude likely sale listings: cap at ₹1.5L unless user set a higher max_price
    if price_max is None or price_max <= 150000:
        query += " AND (l.price IS NULL OR l.price <= 150000)"

    query += " ORDER BY l.extracted_at DESC"

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_stats() -> dict:
    """Get summary statistics."""
    conn = _get_conn()
    stats = {}

    # Rent cap — exclude likely sale listings from stats
    rent_cap = "AND (price IS NULL OR price <= 150000)"

    stats["total_posts"] = conn.execute("SELECT COUNT(*) FROM raw_posts").fetchone()[0]
    stats["total_listings"] = conn.execute(
        f"SELECT COUNT(*) FROM rental_listings WHERE post_type = 'supply' {rent_cap}"
    ).fetchone()[0]
    stats["demand_posts"] = conn.execute(
        "SELECT COUNT(*) FROM rental_listings WHERE post_type = 'demand'"
    ).fetchone()[0]
    stats["other_posts"] = conn.execute(
        "SELECT COUNT(*) FROM rental_listings WHERE post_type = 'other'"
    ).fetchone()[0]
    stats["geocoded"] = conn.execute(
        "SELECT COUNT(*) FROM rental_listings WHERE latitude IS NOT NULL"
    ).fetchone()[0]

    # City breakdown
    rows = conn.execute(
        f"""SELECT city, COUNT(*) as count FROM rental_listings
           WHERE post_type = 'supply' AND city != '' {rent_cap}
           GROUP BY LOWER(city) ORDER BY count DESC"""
    ).fetchall()
    stats["by_city"] = {row["city"]: row["count"] for row in rows}

    # Price range
    row = conn.execute(
        f"SELECT MIN(price) as min_p, MAX(price) as max_p, AVG(price) as avg_p FROM rental_listings WHERE post_type = 'supply' AND price IS NOT NULL {rent_cap}"
    ).fetchone()
    if row and row["min_p"] is not None:
        stats["price_min"] = row["min_p"]
        stats["price_max"] = row["max_p"]
        stats["price_avg"] = round(row["avg_p"])

    # Source breakdown
    source_rows = conn.execute(
        f"""SELECT source, COUNT(*) as count FROM rental_listings
           WHERE post_type = 'supply' AND source != '' {rent_cap}
           GROUP BY source ORDER BY count DESC"""
    ).fetchall()
    stats["by_source"] = {row["source"]: row["count"] for row in source_rows}

    conn.close()
    return stats


# --- Preferences CRUD ---

def save_preferences(prefs: dict) -> str:
    """Save or update user preferences. Returns the preference ID."""
    import uuid as _uuid
    from datetime import datetime

    conn = _get_conn()
    pref_id = prefs.get("id") or _uuid.uuid4().hex[:12]
    now = datetime.now().isoformat()

    conn.execute(
        """INSERT OR REPLACE INTO user_preferences
           (id, city, areas, budget_min, budget_max, bedrooms, furnished, listing_type, is_active, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM user_preferences WHERE id = ?), ?), ?)""",
        (
            pref_id,
            prefs.get("city", "Bangalore"),
            json.dumps(prefs.get("areas", [])),
            prefs.get("budget_min"),
            prefs.get("budget_max"),
            json.dumps(prefs.get("bedrooms", [2, 3])),
            prefs.get("furnished", ""),
            prefs.get("listing_type", ""),
            1 if prefs.get("is_active", True) else 0,
            pref_id,
            now,
            now,
        ),
    )
    conn.commit()
    conn.close()
    return pref_id


def get_preferences() -> list[dict]:
    """Get all user preferences."""
    conn = _get_conn()
    rows = conn.execute("SELECT * FROM user_preferences ORDER BY updated_at DESC").fetchall()
    conn.close()
    result = []
    for row in rows:
        d = dict(row)
        d["areas"] = json.loads(d["areas"]) if d.get("areas") else []
        d["bedrooms"] = json.loads(d["bedrooms"]) if d.get("bedrooms") else []
        d["is_active"] = bool(d.get("is_active"))
        result.append(d)
    return result


def delete_preferences(pref_id: str) -> bool:
    """Delete a preference set."""
    conn = _get_conn()
    cursor = conn.execute("DELETE FROM user_preferences WHERE id = ?", (pref_id,))
    conn.commit()
    conn.close()
    return cursor.rowcount > 0
