#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${1:-.}"
OUT_FILE="${2:-all_js_concat.txt}"

find "$ROOT_DIR" -type f \( \
  -name '*.js' -o -name '*.jsx' -o -name '*.mjs' -o -name '*.cjs' -o \
  -name '*.ts' -o -name '*.tsx' -o -name '*.svelte' \
\) \
  -not -path '*/.git/*' \
  -not -path '*/node_modules/*' \
  -not -path '*/dist/*' \
  -not -path '*/build/*' \
  -not -path '*/coverage/*' \
  -not -path '*/.next/*' \
  -not -path '*/out/*' \
  -not -path '*/.nuxt/*' \
  -not -path '*/.cache/*' \
  -not -path '*/.parcel-cache/*' \
  -not -path '*/.svelte-kit/*' \
  -not -path '*/.idea/*' \
  -not -path '*/.vscode/*' \
  -not -name '*.min.js' \
  | sort \
  | while read -r f; do
      echo "===== FILE: $f ====="
      cat "$f"
      echo
    done > "$OUT_FILE"
