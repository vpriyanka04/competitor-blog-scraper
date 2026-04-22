# Deploying to Zoho Catalyst

Phase 1 code is ready. Everything runs locally via `streamlit run app.py` as
before. To ship to Catalyst, follow the steps below.

## Prerequisites

- Node.js 14+ (for the Catalyst CLI).
- A Catalyst project created in https://catalyst.zoho.com/.
  - Note your **Project ID** (shown in the project URL and Settings).
- Catalyst CLI installed and logged in:
  ```bash
  npm install -g zcatalyst-cli
  catalyst login
  ```

## Files in this repo

| File | Used by | Purpose |
|---|---|---|
| `app.py` | UI (AppSail) | Streamlit dashboard |
| `scrape_job.py` | scrape-job Function | Daily scrape + Cliq notify |
| `notifier.py` | scrape-job Function | Cliq webhook sender |
| `scrapers.py` | both | Per-source scrapers + AEO + keywords |
| `db.py` | both | SQLite helpers (local); swap to Catalyst Data Store later |
| `start.sh` | AppSail | Streamlit launch command (binds to `$PORT`) |
| `requirements.txt` | both | Pinned deps |

## Step 1 — Initialize Catalyst in this directory

```bash
cd /Users/priya-4099/Documents/scrapper
catalyst init
```
Choose:
- Project = your existing Catalyst project
- Add **Functions** (for `scrape_job`)
- Add **AppSail** (for the Streamlit UI)

Catalyst will create `catalyst.json` and scaffolding. Don't commit secrets.

## Step 2 — Wire up the Function

Move `scrape_job.py`, `scrapers.py`, `db.py`, `notifier.py`, and
`requirements.txt` into the `functions/scrape-job/` directory Catalyst scaffolded.

In `functions/scrape-job/catalyst-config.json`, configure:
- **Runtime**: Python 3.9 (or latest available)
- **Handler**: `scrape_job.run` (entry point the Catalyst runtime will invoke)
- **Timeout**: 300 seconds (initial scrapes can take ~60s)

## Step 3 — Wire up AppSail

Move `app.py`, `scrapers.py`, `db.py`, `notifier.py`, `start.sh`, `.streamlit/`,
and `requirements.txt` into the `appsail/ui/` directory.

In the AppSail `app-config.json` (or equivalent):
- **Stack**: Python 3.9+
- **Start command**: `bash start.sh`
- **Port**: read from `$PORT` (already handled by `start.sh`)
- **Auth**: enable Catalyst **Basic Authentication** so only your team can access.

## Step 4 — Set environment variables

In the Catalyst console → **Settings → Environment Variables**, add:

| Key | Value |
|---|---|
| `CLIQ_WEBHOOK_URL` | your Cliq incoming webhook URL (generated in Cliq → channel → Integrations → Incoming Webhooks) |
| `CLIQ_OAUTH_TOKEN` | *(optional)* only if using REST API path, not incoming webhook |

Apply these to **both** the Function and the AppSail service.

## Step 5 — Schedule the scrape-job Function

Catalyst console → **Cron Jobs** → New Cron:
- **Cron expression**: `0 9 * * *` (daily 9 AM)
- **Function**: `scrape-job`

## Step 6 — Deploy

```bash
catalyst deploy
```

After deploy completes, Catalyst prints the AppSail URL. Share that URL (and
the basic-auth credentials) with the marketing team.

## Step 7 — Test the full pipeline

1. From the Catalyst console, manually invoke the `scrape-job` Function.
2. Check its logs — should show `fetched=N new=M cliq: sent: 204`.
3. Check the Cliq channel — a digest message should arrive for any new posts.
4. Open the AppSail URL — you should see the dashboard with the same data.

## Migrating from SQLite (when ready)

Current DB layer uses SQLite (`scrapper.db`). AppSail's filesystem isn't
guaranteed to persist between restarts, so for production this should move to
**Catalyst Data Store**. The swap is localized to `db.py` — same function
signatures, different backend. Save this for a later phase if SQLite
limitations bite.
