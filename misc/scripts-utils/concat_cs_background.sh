#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${1:-.}"
OUT_FILE="${2:-all_cs_concat.txt}"

find "$ROOT_DIR" -type f \( -name '*.cs' -o -name '*.csproj' \) \
  -not -path '*/.git/*' \
  -not -path '*/bin/*' \
  -not -path '*/obj/*' \
  -not -path '*/packages/*' \
  | sort \
  | while read -r f; do
      echo "===== FILE (relative): $f ====="
      cat "$f"
      echo
    done > "$OUT_FILE"