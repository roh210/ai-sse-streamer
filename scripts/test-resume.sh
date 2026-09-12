#!/bin/bash

if ! curl -s http://localhost:3000/api/streams -o /dev/null; then
    echo "Server doesn't seem to be running on localhost:3000 — start it first."
    exit 1
fi

PROMPT="Write a very detailed 1000 word essay about the history of the Roman Empire"

echo "Starting production..."
STREAM_ID=$(curl -s -X POST http://localhost:3000/api/streams \
  -H "Content-Type: application/json" \
  -d "{\"prompt\": \"$PROMPT\"}" | grep -o '"streamId":"[^"]*"' | cut -d'"' -f4)

echo "Stream ID: $STREAM_ID"

echo "Waiting 5 seconds for real tokens to accumulate..."
sleep 5

echo "Reading the first real entry ID directly from Redis (no race)..."
LAST_ID=$(docker exec ai-sse-streamer-redis-1 redis-cli XRANGE stream:$STREAM_ID - + COUNT 1 \
  | head -1)

echo "Resuming from ID: $LAST_ID"
echo ""
echo "=== What the reconnect actually captured ==="
timeout -k 2 5 curl -sN http://localhost:3000/api/streams/$STREAM_ID \
  -H "Last-Event-ID: $LAST_ID"