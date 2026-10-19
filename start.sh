#!/usr/bin/env bash
# Usage: ./start.sh setup | check | monitor | live | positions | report  [extra args]
set -euo pipefail
cd "$(dirname "$0")"
cmd="${1:-monitor}"; shift || true
if [ "$cmd" = "setup" ]; then
  python3 -m venv .venv
  .venv/bin/pip install -U pip
  .venv/bin/pip install -r requirements.txt
  [ -f .env ] || cp .env.example .env
  echo "done. edit .env and config/config.yaml, then: ./start.sh check"
  exit 0
fi
[ -d .venv ] || { echo "run ./start.sh setup first"; exit 1; }
exec .venv/bin/python -m arb "$cmd" "$@"
