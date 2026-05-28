#!/bin/sh
set -e

SNAPSHOT_DIR="${SNAPSHOT_DIR:-/snapshots}"
RETAIN="${RETAIN_COUNT:-24}"
TIMESTAMP=$(date -u +"%Y%m%dT%H%M%SZ")
FILE="$SNAPSHOT_DIR/reconciler_$TIMESTAMP.dump"

mkdir -p "$SNAPSHOT_DIR"

pg_dump \
  --host="$PGHOST" \
  --port="${PGPORT:-5432}" \
  --username="$PGUSER" \
  --dbname="$PGDATABASE" \
  --format=custom \
  --compress=9 \
  --file="$FILE"

echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] snapshot written: $FILE ($(du -sh "$FILE" | cut -f1))"

# prune oldest snapshots beyond retention window
ls -1t "$SNAPSHOT_DIR"/*.dump 2>/dev/null | tail -n "+$((RETAIN + 1))" | while read -r old; do
  rm -f "$old"
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] pruned: $old"
done
