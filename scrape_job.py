"""Standalone scrape + notify entry point.

Runs the full scrape pipeline, detects genuinely new posts (those not already
in the DB), persists them, and posts a digest to the Cliq channel.

Can be invoked three ways:
  1. Directly:      python scrape_job.py
  2. From Catalyst: invoked as a scheduled Catalyst Function
  3. From the UI:   imported and called from a "Refresh + notify" button
"""
import json
import sys
from datetime import datetime

from catalyst_storage import download_db, upload_db
from db import (
    existing_urls,
    init_db,
    insert_post,
    posts_missing_aeo,
    update_aeo,
)
from notifier import send_cliq_digest
from scrapers import SCRAPERS, SOURCE_NAMES, compute_aeo_score

SOURCE_LABELS = {
    "apptics": "Zoho Apptics",
    "mixpanel": "Mixpanel",
    "amplitude": "Amplitude",
    "luciq": "Luciq (Instabug)",
    "sentry": "Sentry",
    "appbot": "Appbot",
}


def _scrape_all():
    """Run every scraper, collecting all returned posts in one flat list.
    Per-source failures are logged but don't halt the batch."""
    all_posts = []
    for scrape in SCRAPERS:
        try:
            posts = scrape()
            all_posts.extend(posts)
            print(f"  {scrape.__name__}: {len(posts)} fetched")
        except Exception as exc:
            print(f"  {scrape.__name__}: FAILED — {exc}", file=sys.stderr)
    return all_posts


def _persist_new(fetched_posts):
    """Insert fetched posts into the DB and return the subset that were
    genuinely new (not previously in the DB)."""
    urls = [p["url"] for p in fetched_posts]
    already_known = existing_urls(urls)
    new_posts = [p for p in fetched_posts if p["url"] not in already_known]
    for post in fetched_posts:
        insert_post(post)  # INSERT OR IGNORE — safe for known URLs
    return new_posts


def _backfill_missing_aeo():
    """Catalyst Function reruns may have partially-scored rows. Re-score any."""
    missing = posts_missing_aeo()
    if not missing:
        return 0
    scored = 0
    for row in missing:
        try:
            score, signals = compute_aeo_score(row["url"])
            if score is not None:
                update_aeo(row["id"], score, json.dumps(signals) if signals else None)
                scored += 1
        except Exception as exc:
            print(f"  aeo-backfill failed for {row['url']}: {exc}", file=sys.stderr)
    return scored


def run():
    print(f"[{datetime.utcnow().isoformat()}Z] scrape_job starting")
    downloaded = download_db()
    print(f"  catalyst File Store download: {'ok' if downloaded else 'skipped/first-run'}")
    init_db()
    fetched = _scrape_all()
    new_posts = _persist_new(fetched)
    backfilled = _backfill_missing_aeo()
    print(f"  fetched={len(fetched)} new={len(new_posts)} aeo_backfilled={backfilled}")
    cliq_result = send_cliq_digest(new_posts, SOURCE_LABELS)
    print(f"  cliq: {cliq_result}")
    uploaded = upload_db()
    print(f"  catalyst File Store upload: {'ok' if uploaded else 'skipped'}")
    return {
        "fetched": len(fetched),
        "new": len(new_posts),
        "aeo_backfilled": backfilled,
        "cliq": cliq_result,
    }


if __name__ == "__main__":
    result = run()
    sys.exit(0 if result else 1)
