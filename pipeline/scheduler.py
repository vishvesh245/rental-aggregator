"""APScheduler setup for automated scraping."""

from __future__ import annotations

import json
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from pipeline.orchestrator import run_full_pipeline
from pipeline.base_scraper import SearchParams
from pipeline.storage import _get_conn


scheduler = BackgroundScheduler()


def setup_scheduler():
    """
    Start the scheduler with a job that runs the pipeline
    for all saved user preferences, twice daily.
    """
    scheduler.add_job(
        _run_scheduled_scrape,
        IntervalTrigger(hours=12),
        id="main_scrape",
        replace_existing=True,
        name="Multi-source rental scrape",
    )
    scheduler.start()
    print("  [Scheduler] Started — running pipeline every 12 hours")


def _run_scheduled_scrape():
    """Run the pipeline for all saved preferences."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM user_preferences WHERE is_active = 1"
    ).fetchall()
    conn.close()

    if not rows:
        print("  [Scheduler] No active preferences found. Skipping.")
        return

    for row in rows:
        pref = dict(row)
        params = SearchParams(
            city=pref.get("city", "Bangalore"),
            areas=json.loads(pref["areas"]) if pref.get("areas") else None,
            budget_min=pref.get("budget_min"),
            budget_max=pref.get("budget_max"),
            bedrooms=json.loads(pref["bedrooms"]) if pref.get("bedrooms") else None,
            furnished=pref.get("furnished", ""),
            max_results=50,
        )

        try:
            print(f"  [Scheduler] Running pipeline for preference: {pref.get('city')} {pref.get('areas')}")
            run_full_pipeline(params=params)
        except Exception as e:
            print(f"  [Scheduler] Pipeline failed for pref {pref.get('id')}: {e}")


def shutdown_scheduler():
    """Shutdown the scheduler gracefully."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        print("  [Scheduler] Shut down")
