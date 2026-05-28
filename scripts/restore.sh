#!/bin/sh
set -e

SNAPSHOT_DIR="${SNAPSHOT_DIR:-/snapshots}"

if [ -n "$1" ]; then
  FILE="$1"
else
  FILE=$(ls -1t "$SNAPSHOT_DIR"/*.dump 2>/dev/null | head -1)
  if [ -z "$FILE" ]; then
    echo "error: no snapshot found in $SNAPSHOT_DIR" >&2
    exit 1
  fi
  echo "no snapshot specified — using latest: $FILE"
fi

if [ ! -f "$FILE" ]; then
  echo "error: snapshot not found: $FILE" >&2
  exit 1
fi

echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] restoring from $FILE ..."

# drop all existing objects then restore
pg_restore \
  --host="$PGHOST" \
  --port="${PGPORT:-5432}" \
  --username="$PGUSER" \
  --dbname="$PGDATABASE" \
  --clean \
  --if-exists \
  --no-owner \
  --no-privileges \
  "$FILE"

echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] restore complete"
