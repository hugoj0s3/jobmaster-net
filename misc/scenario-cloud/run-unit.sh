#!/usr/bin/env bash
# Runs on an instance: one scenario unit (e.g. "ScheduleTest.PostgresPure"). Args: <unit> <timeout-minutes>
# Results in ~/results/<unit>/ (trx, console.log, timeout dumps). Prints the exit code as the last line.
set -uo pipefail
UNIT="$1"
TIMEOUT_MINUTES="${2:-180}"
export DOTNET_ROOT="$HOME/dotnet" PATH="$HOME/dotnet:$PATH" DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1

OUT="$HOME/results/$UNIT"
rm -rf "$OUT" /tmp/jm-scenario-diag
mkdir -p "$OUT"
cd "$HOME/jobmaster-net"

# Load while the unit runs (time, 1-min load average, used memory MB), to tell a starved machine from a real bug.
( while true; do echo "$(date +%T) $(cut -d' ' -f1 /proc/loadavg) $(free -m | awk '/^Mem:/{print $3}')" >>"$OUT/load.log"; sleep 15; done ) &
SAMPLER=$!

# A matrix unit is one theory row, "Matrix.<Scenario>.<Flavor>[.<Second>]" (second: stop type or auth mode);
# anything else is a regular-suite "<Family>.<Variant>" folder.
if [[ "$UNIT" == Matrix.* ]]; then
    IFS='.' read -r _ SCENARIO FLAVOR SECOND <<<"$UNIT"
    PROJECT=tests/JobMaster.ScenarioMatrixTests
    FILTER="FullyQualifiedName~JobMaster.ScenarioMatrixTests.Scenarios.${SCENARIO}.&DisplayName~${FLAVOR}"
    [ -n "${SECOND:-}" ] && FILTER="${FILTER}&DisplayName~${SECOND}"
else
    PROJECT=tests/JobMaster.ScenarioTests
    FILTER="FullyQualifiedName~JobMaster.ScenarioTests.Scenarios.${UNIT}."
fi

timeout "${TIMEOUT_MINUTES}m" dotnet test "$PROJECT" --no-build \
    --filter "$FILTER" \
    --logger "trx;LogFileName=${UNIT}.trx" --logger "console;verbosity=normal" \
    --results-directory "$OUT" >"$OUT/console.log" 2>&1
RC=$?
kill "$SAMPLER" 2>/dev/null

[ -d /tmp/jm-scenario-diag ] && cp -r /tmp/jm-scenario-diag "$OUT/diag"
# Each unit starts its own engines; drop leftovers (a timed-out run can leave containers) before the next one.
docker ps -aq | xargs -r docker rm -f >/dev/null 2>&1
docker network prune -f >/dev/null 2>&1

# 124 = killed by timeout. 3 = exit code 0 but no passed test in the trx (e.g. a filter that matched nothing).
TRX="$OUT/${UNIT}.trx"
if [ $RC -eq 0 ] && { [ ! -f "$TRX" ] || ! grep -q 'outcome="Passed"' "$TRX" || grep -q 'outcome="Failed"' "$TRX"; }; then RC=3; fi
echo "$RC"
