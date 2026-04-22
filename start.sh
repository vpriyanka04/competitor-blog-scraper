#!/bin/bash
# Catalyst AppSail start script. AppSail sets X_ZOHO_CATALYST_LISTEN_PORT;
# we bind Streamlit to it.
exec streamlit run app.py \
  --global.developmentMode false \
  --server.port "${X_ZOHO_CATALYST_LISTEN_PORT:-8501}" \
  --server.address 0.0.0.0 \
  --server.headless true \
  --browser.gatherUsageStats false
