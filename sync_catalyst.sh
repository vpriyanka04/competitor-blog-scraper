#!/bin/bash
# Sync shared Python modules from project root into both Catalyst subdirs.
# Run after any edit to app.py / scrapers.py / db.py / notifier.py /
# scrape_job.py / catalyst_storage.py at the root, BEFORE `catalyst deploy`.
set -e
ROOT="$(cd "$(dirname "$0")" && pwd)"

# Files shared by both AppSail and the Function
SHARED=(scrapers.py db.py notifier.py catalyst_storage.py)

# AppSail-only
APPSAIL_ONLY=(app.py start.sh)

# Function-only
FUNCTION_ONLY=(scrape_job.py)

for f in "${SHARED[@]}" "${APPSAIL_ONLY[@]}"; do
  cp "$ROOT/$f" "$ROOT/appsail-python/$f"
done
cp -r "$ROOT/.streamlit" "$ROOT/appsail-python/" 2>/dev/null || true

for f in "${SHARED[@]}" "${FUNCTION_ONLY[@]}"; do
  cp "$ROOT/$f" "$ROOT/functions/scrape_job/$f"
done

echo "synced root → appsail-python/ and functions/scrape_job/"
