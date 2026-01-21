#!/usr/bin/env bash
set -euo pipefail

SERVER="${SERVER:-nats://jmuser:jmpass@localhost:4222}"
STREAM_GREP="${STREAM_GREP:-JobMasterNatJetStreams_}"   # pattern to match your stream(s)
SINCE="${SINCE:-15m}"                                   # time window: e.g. 10m, 1h
COUNT="${COUNT:-300}"                                   # messages to scan in view

echo "SERVER=$SERVER  STREAM_GREP=$STREAM_GREP  SINCE=$SINCE  COUNT=$COUNT"
echo

# 1) Discover streams
STREAMS=$(nats stream ls --names --json \
          | jq -r '.[]' \
          | grep -E "$STREAM_GREP" || true)
if [ -z "$STREAMS" ]; then
  echo "No streams found matching pattern: $STREAM_GREP"
  exit 0
fi

for STREAM in $STREAMS; do
  echo "================ STREAM: $STREAM ================"
  nats stream info "$STREAM" || true
  echo

  echo "-- Recent subjects in last $SINCE (up to $COUNT msgs) --"
  # Fallback for older nats CLI (no --json/--headers support on view): parse plain text
  nats stream view "$STREAM" --since "$SINCE" \
  | awk -v max="$COUNT" '/^Subject: /{print $2; c++; if (c>=max) exit }' \
  | sort | uniq -c | sort -nr | head -n 30
  echo

  echo "-- Consumers summary (name, filter, deliver, pending) --"
  CONS=$(nats consumer ls "$STREAM" --json | jq -r '.[].name' || true)
  if [ -z "$CONS" ]; then
    echo "(no consumers)"
  else
    while read -r NAME; do
      [ -z "$NAME" ] && continue
      nats consumer info "$STREAM" "$NAME" --json \
      | jq -r '"NAME=\(.config.durable_name // .name)  FILTER=\(.config.filter_subject)  DELIVER=\(.config.deliver_policy)  ACK_WAIT=\(.config.ack_wait)  MAX_ACK_PENDING=\(.config.max_ack_pending)  PENDING=\(.num_pending)"'
    done <<< "$CONS"
  fi
  echo

  echo "-- Per-filter recent sample (first 5 seen) --"
  # Correlate each consumer filter with recent subjects
  if [ -n "$CONS" ]; then
    while read -r NAME; do
      [ -z "$NAME" ] && continue
      FILTER=$(nats consumer info "$STREAM" "$NAME" --json | jq -r '.config.filter_subject // ""')
      [ -z "$FILTER" ] && continue
      echo "Filter: $FILTER"
      nats stream view "$STREAM" --since "$SINCE" \
      | awk -v max="$COUNT" '/^Subject: /{print $2; c++; if (c>=max) exit }' \
      | grep -F "$FILTER" | head -n 5
      echo
    done <<< "$CONS"
  fi

  echo "-- Correlation ID duplicate scan: skipped (CLI lacks --json headers on view). Use nats-box for this step. --"
  echo
done