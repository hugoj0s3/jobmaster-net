#!/usr/bin/env bash
# Prints the scenario units as a JSON array, longest first, for the workflow's matrix. Args: [<regex>]
# A unit is one theory row of tests/JobMaster.ScenarioMatrixTests, "<Scenario>.<Flavor>[.<StopType|Auth>]", e.g.
#   ...Scenarios.DrainModeTest.DrainModeTests.Run(flavor: "PostgresPure", stopType: Crash) -> DrainModeTest.PostgresPure.Crash
# Only the scenarios' Run rows (the Flavors/* helper tests are left out). Fails when nothing matches.
set -euo pipefail
UNIT_FILTER="${1:-}"
cd "$(dirname "$0")/../.."

# Rough minutes per scenario, so the longest units start first (the rest fill the free runners).
estimate() {
    case "$1" in
        ScheduleTest.*) echo 75 ;;
        RecurringScheduleTest.*|DataRetentionArchiveTest.*) echo 22 ;;
        DrainModeTest.*) echo 15 ;;
        ArchivedModeTest.*|StandaloneToDistributedTest.*) echo 12 ;;
        ConfirmOnMasterTest.*|FallbackBucketTest.*|MigratingModeTest.*) echo 6 ;;
        *) echo 2 ;;
    esac
}

dotnet build tests/JobMaster.ScenarioMatrixTests -v q -nologo >/dev/null
UNITS="$(dotnet test tests/JobMaster.ScenarioMatrixTests --no-build --list-tests 2>/dev/null | tr -d '\r' \
    | sed -nE 's/^ *JobMaster\.ScenarioMatrixTests\.Scenarios\.([A-Za-z]+)\.[A-Za-z]+\.Run\(([^)]*)\).*/\1|\2/p' \
    | while IFS='|' read -r scenario args; do
        echo "$scenario.$(echo "$args" | sed -E 's/[a-zA-Z]+: //g; s/"//g; s/, /./g')"
    done | sort -u)"

SELECTED="$(while read -r unit; do
    [ -z "$UNIT_FILTER" ] || [[ "$unit" =~ $UNIT_FILTER ]] || continue
    echo "$(estimate "$unit") $unit"
done <<<"$UNITS" | sort -rn | cut -d' ' -f2)"

if [ -z "$SELECTED" ]; then echo "No units match '$UNIT_FILTER'." >&2; exit 2; fi
# Unit names are letters, digits and dots only, so quoting each one is enough for valid JSON.
echo "[$(echo "$SELECTED" | sed 's/.*/"&"/' | paste -sd, -)]"
