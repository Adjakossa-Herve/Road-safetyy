#!/usr/bin/env bash
set -euo pipefail

# Lightweight helper to run the installed console script via Poetry
if ! command -v poetry >/dev/null 2>&1; then
  echo "Poetry n'est pas installé. Installez Poetry et lancez 'poetry install' puis réessayez." >&2
  exit 1
fi

if [ "$#" -eq 0 ]; then
  echo "Usage: $0 <command> [args...]"
  echo "Exemple: $0 chat"
  exit 1
fi

poetry run road-safety "$@"
