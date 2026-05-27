#!/bin/bash
# Collects basic system metrics and broker /metrics endpoint periodically
# Usage: collect_metrics.sh <metrics_url> <interval_seconds> <out_prefix>
METRICS_URL=${1:-http://localhost:9000/metrics}
INT=${2:-5}
OUT=${3:-metrics}
mkdir -p $OUT
COUNT=0
while true; do
  ts=$(date +%s)
  echo "[$ts] collecting..."
  vmstat 1 2 | tail -1 > $OUT/vmstat_$COUNT.txt
  if command -v iostat >/dev/null 2>&1; then iostat -x 1 1 > $OUT/iostat_$COUNT.txt; fi
  curl -s $METRICS_URL > $OUT/metrics_$COUNT.txt || echo "curl failed" > $OUT/metrics_$COUNT.txt
  sleep $INT
  COUNT=$((COUNT+1))
done
