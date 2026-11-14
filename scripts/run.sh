#!/usr/bin/env bash
# Запускает поток Prefect на edge-ноде.
set -euo pipefail
cd "$(dirname "$0")/.."
exec .venv/bin/python flows/pipeline.py "$@"
