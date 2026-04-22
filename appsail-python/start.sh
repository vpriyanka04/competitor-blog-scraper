#!/bin/bash
# Catalyst AppSail start script.
# Deps are pre-vendored into ./vendor because Catalyst's pip can't resolve
# source-only packages (sgmllib3k, breadability). Inject vendor/ onto PYTHONPATH.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export PYTHONPATH="$SCRIPT_DIR/vendor:${PYTHONPATH:-}"
# --global.developmentMode=false because vendored installs look like "dev"
# to Streamlit. CORS/XSRF off because Catalyst's reverse proxy rewrites
# origin headers which otherwise trips Streamlit's checks.
exec python3 -m streamlit run app.py \
  --global.developmentMode false \
  --server.port "${X_ZOHO_CATALYST_LISTEN_PORT:-8501}" \
  --server.address 0.0.0.0 \
  --server.headless true \
  --server.enableCORS false \
  --server.enableXsrfProtection false \
  --server.enableWebsocketCompression false \
  --browser.gatherUsageStats false
