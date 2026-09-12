#!/usr/bin/env bash
set -euo pipefail

PROJECT="Tests/JobMaster.IntegrationTests/JobMaster.IntegrationTests.csproj"

# RepoConformance runs against master DBs only (no NATS, no Mixed)
MASTER_DBS=(Postgres MySql SqlServer)

# Scheduler / Recurring / DrainMode cover all backends
ALL_DBS=(Postgres MySql SqlServer Nats Mixed)

EXITCODE=0

echo "============================================================"
echo " JobMaster Integration Tests"
echo "============================================================"
echo

echo "Building..."
dotnet build "$PROJECT" -q
echo "Build OK"
echo

# ── 1. RepoConformance ───────────────────────────────────────
echo "[1/5] RepoConformance"
echo "============================================================"
for DB in "${MASTER_DBS[@]}"; do
    echo
    echo "--- RepoConformance : $DB"
    if dotnet test "$PROJECT" --no-build --filter "DB=$DB&FullyQualifiedName~RepoConformance" --logger "console;verbosity=normal"; then
        echo "PASSED"
    else
        echo "FAILED"
        EXITCODE=1
    fi
done

# ── 2. Recurring ─────────────────────────────────────────────
echo
echo "[2/5] Recurring"
echo "============================================================"
for DB in "${ALL_DBS[@]}"; do
    echo
    echo "--- Recurring : $DB"
    if dotnet test "$PROJECT" --no-build --filter "DB=$DB&TestType=Recurring" --logger "console;verbosity=normal"; then
        echo "PASSED"
    else
        echo "FAILED"
        EXITCODE=1
    fi
done

# ── 3. Scheduler ─────────────────────────────────────────────
echo
echo "[3/5] Scheduler"
echo "============================================================"
for DB in "${ALL_DBS[@]}"; do
    echo
    echo "--- Scheduler : $DB"
    if dotnet test "$PROJECT" --no-build --filter "DB=$DB&TestType=Scheduler" --logger "console;verbosity=normal"; then
        echo "PASSED"
    else
        echo "FAILED"
        EXITCODE=1
    fi
done

# ── 4. DrainMode Theory 1 (10000 jobs) ───────────────────────
echo
echo "[4/5] DrainMode - Theory 1 (10000 jobs)"
echo "============================================================"
for DB in "${ALL_DBS[@]}"; do
    echo
    echo "--- DrainMode Theory 1 : $DB"
    if dotnet test "$PROJECT" --no-build --filter "DB=$DB&TestType=DrainMode&DisplayName~10000," --logger "console;verbosity=normal"; then
        echo "PASSED"
    else
        echo "FAILED"
        EXITCODE=1
    fi
done

# ── 5. DrainMode Theory 2 (100000 jobs) ──────────────────────
echo
echo "[5/5] DrainMode - Theory 2 (100000 jobs)"
echo "============================================================"
for DB in "${ALL_DBS[@]}"; do
    echo
    echo "--- DrainMode Theory 2 : $DB"
    if dotnet test "$PROJECT" --no-build --filter "DB=$DB&TestType=DrainMode&DisplayName~100000" --logger "console;verbosity=normal"; then
        echo "PASSED"
    else
        echo "FAILED"
        EXITCODE=1
    fi
done

echo
echo "============================================================"
if [ "$EXITCODE" -eq 0 ]; then
    echo " ALL TESTS PASSED"
else
    echo " SOME TESTS FAILED"
fi
echo "============================================================"
exit $EXITCODE
