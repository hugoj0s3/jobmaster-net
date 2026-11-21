#!/usr/bin/env bash
# Runs one scenario unit (e.g. "ScheduleTest.PostgresPure") on a GitHub runner, after the matrix project is built.
# Args: <unit> <timeout-minutes> <results-dir>
# Results in <results-dir>/<unit>/ (trx, console.log, load.log, timeout dumps). Exits with the unit's result:
# 0 passed, 124 timed out, 3 exit code 0 but no passed test in the trx (e.g. a filter that matched nothing).
set -uo pipefail
UNIT="$1"
TIMEOUT_MINUTES="$2"
OUT="$3/$UNIT"
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1
rm -rf "$OUT" /tmp/jm-scenario-diag
mkdir -p "$OUT"
cd "$(dirname "$0")/../.." || exit 2

# Load while the unit runs (time, 1-min load average, used memory MB), to tell a starved runner from a real bug.
( while true; do echo "$(date +%T) $(cut -d' ' -f1 /proc/loadavg) $(free -m | awk '/^Mem:/{print $3}')" >>"$OUT/load.log"; sleep 15; done ) &
SAMPLER=$!

IFS='.' read -r SCENARIO FLAVOR SECOND <<<"$UNIT"
FILTER="FullyQualifiedName~JobMaster.ScenarioMatrixTests.Scenarios.${SCENARIO}.&DisplayName~${FLAVOR}"
[ -n "${SECOND:-}" ] && FILTER="${FILTER}&DisplayName~${SECOND}"

timeout "${TIMEOUT_MINUTES}m" dotnet test tests/JobMaster.ScenarioMatrixTests --no-build \
    --filter "$FILTER" \
    --logger "trx;LogFileName=${UNIT}.trx" --logger "console;verbosity=normal" \
    --results-directory "$OUT" 2>&1 | tee "$OUT/console.log"
RC=${PIPESTATUS[0]}
kill "$SAMPLER" 2>/dev/null

[ -d /tmp/jm-scenario-diag ] && cp -r /tmp/jm-scenario-diag "$OUT/diag"

TRX="$OUT/${UNIT}.trx"
if [ $RC -eq 0 ] && { [ ! -f "$TRX" ] || ! grep -q 'outcome="Passed"' "$TRX" || grep -q 'outcome="Failed"' "$TRX"; }; then RC=3; fi
awk '{ if ($2 > l) l = $2; if ($3 > m) m = $3 } END { printf "Peak load %.1f, memory %.1f GB\n", l, m / 1024 }' "$OUT/load.log"
exit $RC
