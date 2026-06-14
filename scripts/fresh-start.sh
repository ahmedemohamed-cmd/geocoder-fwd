#!/usr/bin/env bash
#
# Tear the stack down, wipe the generated data/intermediate artifacts, then
# rebuild and bring everything back up (including the one-shot Zitadel init).
#
# This is destructive: it removes Docker volumes (-v) and the generated files
# under ./data. Raw inputs you dropped in (e.g. *.osm.pbf) are NOT touched.
#
# Usage:
#   scripts/fresh-start.sh          # CPU-only (standard) stack
#   scripts/fresh-start.sh --ai     # GPU + vectors/AI stack
set -euo pipefail

# Resolve the repo root from this script's location, so it works from anywhere.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE=(docker compose -f docker-compose.yaml)
if [[ "${1:-}" == "--ai" ]]; then
  COMPOSE+=(-f docker-compose.ai.yaml)
fi

DATA_DIR="$ROOT/data"

# Generated/intermediate artifacts that must be cleared for a clean re-import.
GENERATED=(
  admin_data
  duplicateways.txt
  file_hashes.txt
  .processed
  traffic.tar.tar
  valhalla.json
  valhalla
  valhalla_tiles
  valhalla_tiles.tar
  geonames/.processed
  openaddresses/.processed
)

echo "==> Stopping stack and removing volumes"
"${COMPOSE[@]}" down -v

echo "==> Clearing generated data under $DATA_DIR"
for item in "${GENERATED[@]}"; do
  target="$DATA_DIR/$item"
  if [[ -e "$target" ]]; then
    echo "    rm -rf $item"
    sudo rm -rf "$target"
  fi
done

echo "==> Building images"
"${COMPOSE[@]}" build

echo "==> Starting stack"
"${COMPOSE[@]}" up -d

echo "==> Running one-shot Zitadel init"
"${COMPOSE[@]}" run --rm billing-zitadel-init

echo "==> Fresh start complete"
