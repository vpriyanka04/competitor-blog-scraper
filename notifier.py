"""Cliq notifier — POSTs a digest of new blog posts to a Zoho Cliq channel.

Supports two auth modes:
  1. Incoming Webhook URL (has ?zapikey=... token in the URL). No extra header.
  2. Cliq REST API URL + OAuth token. Sends 'Authorization: Zoho-oauthtoken <tok>'.
"""
import json
import os
from datetime import datetime

import requests

CLIQ_WEBHOOK_URL = os.environ.get("CLIQ_WEBHOOK_URL", "").strip()
CLIQ_OAUTH_TOKEN = os.environ.get("CLIQ_OAUTH_TOKEN", "").strip()


def _cliq_headers():
    headers = {"Content-Type": "application/json"}
    if CLIQ_OAUTH_TOKEN:
        headers["Authorization"] = f"Zoho-oauthtoken {CLIQ_OAUTH_TOKEN}"
    return headers


def _format_digest(new_posts, source_labels):
    """Build a Cliq message (Cliq accepts 'text' + optional rich cards)."""
    if not new_posts:
        return None

    by_source = {}
    for p in new_posts:
        by_source.setdefault(p["source"], []).append(p)

    lines = [
        f"*Competitor Blog Update — {datetime.utcnow().strftime('%b %d, %Y')}*",
        f"{len(new_posts)} new post(s) detected across {len(by_source)} source(s).",
        "",
    ]
    for source, posts in by_source.items():
        label = source_labels.get(source, source.title())
        lines.append(f"*{label}* — {len(posts)} new")
        for p in posts[:10]:
            score = p.get("aeo_score")
            score_txt = f"  _(Content score {score}/10)_" if score else ""
            lines.append(f"• [{p['title']}]({p['url']}){score_txt}")
        if len(posts) > 10:
            lines.append(f"  …and {len(posts) - 10} more")
        lines.append("")
    return {"text": "\n".join(lines).rstrip()}


def send_cliq_digest(new_posts, source_labels):
    if not CLIQ_WEBHOOK_URL:
        return "skipped: CLIQ_WEBHOOK_URL not set"
    payload = _format_digest(new_posts, source_labels)
    if not payload:
        return "skipped: no new posts"
    r = requests.post(CLIQ_WEBHOOK_URL, headers=_cliq_headers(), data=json.dumps(payload), timeout=15)
    r.raise_for_status()
    return f"sent: {r.status_code}"
