"""Catalyst Cron Function entry point — runs scrape_job daily.

Deps are pre-vendored into ./vendor (linux-x86 wheels) because Catalyst's
pip can't resolve source-only packages like sgmllib3k/breadability. We
inject the vendor dir onto sys.path BEFORE any application imports.
"""
import logging
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_VENDOR = os.path.join(_HERE, "vendor")
if os.path.isdir(_VENDOR) and _VENDOR not in sys.path:
    sys.path.insert(0, _VENDOR)

from scrape_job import run as run_scrape  # noqa: E402 — must come after sys.path setup


def handler(cron_details, context):
    logger = logging.getLogger()
    try:
        result = run_scrape()
        logger.info(
            "scrape_job complete: fetched=%s new=%s aeo_backfilled=%s cliq=%s",
            result["fetched"], result["new"], result["aeo_backfilled"], result["cliq"],
        )
        context.close_with_success()
    except Exception as exc:  # noqa: BLE001 — surface any failure to Catalyst
        logger.exception("scrape_job failed: %s", exc)
        context.close_with_failure()
