#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${1:-.}"
OUT_FILE="${2:-all_cs_concat.txt}"

find "$ROOT_DIR" -type f \( -name '*.cs' -o -name '*.csproj' \) \
  -not -path '*/.git/*' \
  -not -path '*/bin/*' \
  -not -path '*/obj/*' \
  -not -path '*/packages/*' \
  -not -path '*/node_modules/*' \
  -not -path '*/.vs/*' \
  -not -path '*/.idea/*' \
  | sort \
  | while read -r f; do
      echo "===== FILE: $f ====="
      cat "$f"
      echo
    done > "$OUT_FILE"