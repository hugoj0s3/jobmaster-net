#!/usr/bin/env bash
set -euo pipefail

# Usage: ./diff_concat.sh [base-ref] [output-file]
#   base-ref   : git ref to diff against (default: HEAD, shows unstaged+staged changes)
#                Use a branch name or commit to diff from that point, e.g. master or HEAD~1
#   output-file: where to write the result (default: diff_concat.txt)
#
# Examples:
#   ./diff_concat.sh                  # uncommitted changes vs HEAD
#   ./diff_concat.sh master           # all changes on current branch vs master
#   ./diff_concat.sh HEAD~3           # changes in last 3 commits

BASE_REF="${1:-}"
OUT_FILE="${2:-diff_concat.txt}"

if [[ -n "$BASE_REF" ]]; then
    CHANGED_FILES=$(git diff --name-only "$BASE_REF"...HEAD 2>/dev/null || git diff --name-only "$BASE_REF")
else
    # Staged + unstaged changes vs HEAD
    CHANGED_FILES=$(git diff --name-only HEAD)
fi

if [[ -z "$CHANGED_FILES" ]]; then
    echo "No changed files found."
    exit 0
fi

> "$OUT_FILE"

while IFS= read -r f; do
    [[ -f "$f" ]] || continue
    # Only include .cs and .csproj files
    [[ "$f" == *.cs ]] || [[ "$f" == *.csproj ]] || continue
    # Exclude generated/tool directories
    [[ "$f" == */.git/* ]]         && continue
    [[ "$f" == */bin/* ]]          && continue
    [[ "$f" == */obj/* ]]          && continue
    [[ "$f" == */packages/* ]]     && continue
    [[ "$f" == */node_modules/* ]] && continue
    [[ "$f" == */.vs/* ]]          && continue
    [[ "$f" == */.idea/* ]]        && continue
    echo "===== FILE: $f =====" >> "$OUT_FILE"
    cat "$f" >> "$OUT_FILE"
    echo >> "$OUT_FILE"
done <<< "$CHANGED_FILES"

echo "Written to $OUT_FILE"
