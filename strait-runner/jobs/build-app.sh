#!/bin/sh
set -eu

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf '%s\n' "Missing required command: $1" >&2
    exit 1
  fi
}

require_env() {
  if [ -z "${1:-}" ]; then
    printf '%s\n' "Missing required environment variable: $2" >&2
    exit 1
  fi
}

require_env "${INPUT_SOURCE:-}" INPUT_SOURCE
require_env "${STRAIT_WORKDIR:-}" STRAIT_WORKDIR
require_env "${STRAIT_OUTPUT_DIR:-}" STRAIT_OUTPUT_DIR

require_command gleam
require_command npm
require_command tar

SOURCE_ARCHIVE="$INPUT_SOURCE"
SOURCE_DIR="$STRAIT_WORKDIR/source"
ARTIFACT="$STRAIT_OUTPUT_DIR/app.tar.gz"
STAGE_DIR="$STRAIT_WORKDIR/build/app-bundle"

printf '%s\n' "Extracting source archive..."
rm -rf "$SOURCE_DIR"
mkdir -p "$SOURCE_DIR"
tar -xzf "$SOURCE_ARCHIVE" -C "$SOURCE_DIR"

SHIPMENT_DIR="$SOURCE_DIR/glot_backend/build/erlang-shipment"

printf '%s\n' "Building frontend assets..."
(
  cd "$SOURCE_DIR/glot_frontend"
  gleam build
  npm run build
)

printf '%s\n' "Exporting backend Erlang shipment..."
(
  cd "$SOURCE_DIR/glot_backend"
  gleam export erlang-shipment
)

if [ ! -x "$SHIPMENT_DIR/entrypoint.sh" ]; then
  printf '%s\n' "Expected shipment entrypoint not found: $SHIPMENT_DIR/entrypoint.sh" >&2
  exit 1
fi

if [ ! -d "$SHIPMENT_DIR/glot_backend/priv/db/migrations" ]; then
  printf '%s\n' "Expected migrations not found in shipment." >&2
  exit 1
fi

if [ ! -d "$SHIPMENT_DIR/glot_backend/priv/static" ]; then
  printf '%s\n' "Expected frontend static assets not found in shipment." >&2
  exit 1
fi

printf '%s\n' "Packaging $ARTIFACT..."
rm -rf "$STAGE_DIR"
mkdir -p "$STAGE_DIR"
cp -R "$SHIPMENT_DIR" "$STAGE_DIR/app"
chmod +x "$STAGE_DIR/app/entrypoint.sh"

rm -f "$ARTIFACT"
tar -czf "$ARTIFACT" -C "$STAGE_DIR" app

printf '%s\n' "Created $ARTIFACT"
