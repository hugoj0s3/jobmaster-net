#!/usr/bin/env bash
find . -type f \( -name '*.cs' -o -name '*.csproj' \) \
  -not -path './.git/*' -not -path './bin/*' -not -path './obj/*' -not -path './packages/*' \
  | sort \
  | while read -r f; do
      echo "===== FILE (relative): $f ====="
      cat "$f"
      echo
    done > all_cs_concat.txt