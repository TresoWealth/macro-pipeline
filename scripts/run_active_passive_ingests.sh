#!/bin/bash
# Treso Analytics - Active/Passive Monthly Ingests
# Runs every Sunday at 6:35 PM IST (5 min after main macro pipeline)
# Only runs monthly-frequency sources (AMFI, mfapi.in)

set -e

LOGDIR="/home/ubuntu/clawd/logs/treso_analytics"
mkdir -p "$LOGDIR"

cd /home/ubuntu/clawd/treso_analytics
export PYTHONPATH=/home/ubuntu/clawd/treso_analytics

echo "=== Active/Passive Monthly Ingest: $(date -Iseconds) ==="

python3 -c "from ingest_amfi_monthly import ingest; ingest()" \
    >> "$LOGDIR/amfi_monthly.log" 2>&1 &
PID_AMFI=$!

python3 -c "from ingest_mfapi_universe import ingest; ingest()" \
    >> "$LOGDIR/mfapi_universe.log" 2>&1 &
PID_MFAPI=$!

# Run in parallel but wait for both
wait $PID_AMFI
RC_AMFI=$?
wait $PID_MFAPI
RC_MFAPI=$?

echo "AMFI exit: $RC_AMFI | mfapi exit: $RC_MFAPI"
echo "=== Active/Passive Monthly Ingest Complete: $(date -Iseconds) ==="

# Non-zero exit if either failed (for monitoring)
[ $RC_AMFI -eq 0 ] && [ $RC_MFAPI -eq 0 ] && exit 0 || exit 1
