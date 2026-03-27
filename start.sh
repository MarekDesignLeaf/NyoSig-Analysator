#!/bin/bash
export NYOSIG_PROJECT_ROOT=${NYOSIG_PROJECT_ROOT:-/app}
mkdir -p $NYOSIG_PROJECT_ROOT/db $NYOSIG_PROJECT_ROOT/config $NYOSIG_PROJECT_ROOT/cache $NYOSIG_PROJECT_ROOT/logs

# Start API in background
uvicorn nyosig_api:app --host 0.0.0.0 --port 8000 &

# Wait for API to be ready
sleep 3

# Start Streamlit dashboard (foreground)
streamlit run nyosig_dashboard.py \
    --server.port ${PORT:-8501} \
    --server.address 0.0.0.0 \
    --server.headless true \
    --browser.gatherUsageStats false
