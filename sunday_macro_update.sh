#!/bin/bash
# Treso Analytics - Sunday Evening Macro Data Update
# Runs every Sunday at 6 PM IST (12:30 PM UTC)
# Calls the full 7-step pipeline in one shot.

set -e

LOG_DIR="/home/ubuntu/clawd/logs/treso_analytics"
mkdir -p "$LOG_DIR"

LOG_FILE="$LOG_DIR/sunday_macro_update_$(date +%Y%m%d).log"

echo "========================================" | tee -a "$LOG_FILE"
echo "SUNDAY MACRO DATA UPDATE" | tee -a "$LOG_FILE"
echo "Timestamp: $(date -Iseconds)" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

cd /home/ubuntu/clawd/treso_analytics

python3 -c "
from macro_pipeline.macro_data_scheduler import MacroDataScheduler
ok = MacroDataScheduler().run_full_pipeline()
exit(0 if ok else 1)
" 2>&1 | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
echo "✅ SUNDAY UPDATE COMPLETE" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
