#!/bin/bash
# Producer-side rate control check. This is NOT the Phase 21 end-to-end throughput benchmark.
# It only answers: does --rate do what it says, and what does the producer manage unthrottled.
set -euo pipefail

cd "$(dirname "$0")/.."
PY=.venv/bin/python
LIMIT=${LIMIT:-30000}
OUT=benchmarks/raw/producer_rate.txt

{
  echo "producer rate control check"
  echo "date: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "limit: $LIMIT events per run"
  echo "note: producer side only, no consumer, no spark. Not an end to end throughput number."
  echo

  for rate in 500 2000 10000; do
    echo "=== requested ${rate} events/sec ==="
    $PY producer/simulator.py --limit "$LIMIT" --rate "$rate" 2>&1 | grep -E 'emitted|effective'
    echo
  done

  echo "=== unthrottled ceiling, rate set far above capability ==="
  $PY producer/simulator.py --limit "$LIMIT" --rate 1000000 2>&1 | grep -E 'emitted|effective'
  echo

  echo "=== burst mode, 1000 base with 8000 bursts every 5s for 2s ==="
  $PY producer/simulator.py --limit "$LIMIT" --rate 1000 --burst-rate 8000 \
    --burst-every 5 --burst-duration 2 2>&1 | grep -E 'emitted|effective'
} | tee "$OUT"

echo
echo "wrote $OUT"
