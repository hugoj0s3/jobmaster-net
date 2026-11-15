#!/usr/bin/env bash
# Runs the scenario tests on N fresh EC2 instances in parallel, from this PC's working tree (uncommitted
# changes included), then terminates them. Usage (Git Bash, from the repo root):
#
#   misc/scenario-cloud/run-scenarios.sh [-n 10] [-t m5.2xlarge] [-u 'ScheduleTest|BasicExecution'] [--keep-failed] [--list]
#
#   -n / -t        instance count / type (default 10 x m5.2xlarge = 80 vCPU, the account's on-demand quota)
#   -u <regex>     only units matching it (a unit is "<Family>.<Variant>", e.g. ScheduleTest.PostgresPure)
#   --keep-failed  don't terminate instances that ran a failed unit (they still self-terminate after MAX_MINUTES)
#   --list         print the units, longest first, and exit (no AWS calls)
#
# Results: artifacts/scenario-runs/<run-id>/<unit>/ (trx, console.log, diag) and summary.txt.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
INSTANCES=10
TYPE="m5.2xlarge"
UNIT_FILTER=""
KEEP_FAILED=0
LIST_ONLY=0
MAX_MINUTES=330          # hard cap per instance (self-terminate)
UNIT_TIMEOUT_MINUTES=150 # one unit; ScheduleTest takes ~65-75 min locally

while [ $# -gt 0 ]; do
    case "$1" in
        -n) INSTANCES="$2"; shift 2 ;;
        -t) TYPE="$2"; shift 2 ;;
        -u) UNIT_FILTER="$2"; shift 2 ;;
        --keep-failed) KEEP_FAILED=1; shift ;;
        --list) LIST_ONLY=1; shift ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

# Rough minutes per family, only to start the longest units first (the queue balances the rest).
estimate() {
    case "$1" in
        ScheduleTest.*) echo 70 ;;
        RecurringScheduleTest.*|DrainModeTest.*) echo 25 ;;
        ArchivedModeTest.PostgresPure) echo 30 ;;
        ArchivedModeTest.*|StandaloneToDistributedTest.*) echo 20 ;;
        MigratingModeTest.*|FallbackBucketTest.*) echo 15 ;;
        ClusterIdTest.*|ConfirmOnMasterTest.*) echo 10 ;;
        *) echo 5 ;;
    esac
}

# Units from the folders: Scenarios/<Family>/<Variant>/ (Phase folders excluded), or the family itself.
list_units() {
    local scenarios="$REPO/tests/JobMaster.ScenarioTests/Scenarios" family variant found
    for familyDir in "$scenarios"/*/; do
        family="$(basename "$familyDir")"
        found=0
        for variantDir in "$familyDir"*/; do
            [ -d "$variantDir" ] || continue
            variant="$(basename "$variantDir")"
            [[ "$variant" =~ ^Phase[0-9]+$ ]] && continue
            echo "$family.$variant"; found=1
        done
        [ $found -eq 0 ] && echo "$family"
    done
}

UNITS=()
while read -r unit; do
    [ -z "$UNIT_FILTER" ] || [[ "$unit" =~ $UNIT_FILTER ]] || continue
    UNITS+=("$(estimate "$unit") $unit")
done < <(list_units)
mapfile -t UNITS < <(printf '%s\n' "${UNITS[@]}" | sort -rn | cut -d' ' -f2)

if [ ${#UNITS[@]} -eq 0 ]; then echo "No units match '$UNIT_FILTER'." >&2; exit 2; fi
if [ $LIST_ONLY -eq 1 ]; then printf '%s\n' "${UNITS[@]}"; echo "(${#UNITS[@]} units)"; exit 0; fi
[ "$INSTANCES" -gt ${#UNITS[@]} ] && INSTANCES=${#UNITS[@]}

[ -f "$HERE/scenario-cloud.settings.sh" ] || { echo "Missing $HERE/scenario-cloud.settings.sh (see the .example)." >&2; exit 2; }
# shellcheck source=/dev/null
source "$HERE/scenario-cloud.settings.sh"

RUN_ID="scn-$(date +%Y%m%d-%H%M%S)"
OUT="$REPO/artifacts/scenario-runs/$RUN_ID"
mkdir -p "$OUT"
SSH_OPTS=(-i "$PEM_PATH" -o StrictHostKeyChecking=no -o UserKnownHostsFile="$OUT/known_hosts" -o ConnectTimeout=10 -o ServerAliveInterval=60 -o LogLevel=ERROR)
log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT/run.log"; }

# The Windows AWS CLI ends lines with \r; every value read from it goes through this.
aws_text() { aws "$@" --output text | tr -d '\r'; }

INSTANCE_IDS=()
LAUNCHED=0
terminate() {
    [ $LAUNCHED -eq 0 ] && return
    # Looked up by the run's tag, not from INSTANCE_IDS, so nothing is missed even if launching or parsing failed.
    local keep=() ids=() all
    all="$(aws_text ec2 describe-instances \
        --filters "Name=tag:ScenarioRun,Values=$RUN_ID" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
        --query "Reservations[].Instances[].InstanceId")"
    if [ $KEEP_FAILED -eq 1 ] && [ -f "$OUT/failed-instances.txt" ]; then mapfile -t keep < "$OUT/failed-instances.txt"; fi
    for id in $all; do [[ " ${keep[*]} " == *" $id "* ]] || ids+=("$id"); done
    if [ ${#ids[@]} -gt 0 ]; then
        log "Terminating ${#ids[@]} instance(s)..."
        aws ec2 terminate-instances --instance-ids "${ids[@]}" --output text >/dev/null \
            || log "TERMINATE FAILED -- check the AWS console for tag ScenarioRun=$RUN_ID."
    fi
    [ ${#keep[@]} -gt 0 ] && log "Kept (failed units): ${keep[*]} -- they self-terminate within ${MAX_MINUTES} min of launch."
}
trap terminate EXIT
trap 'log "Interrupted."; exit 130' INT TERM

log "Run $RUN_ID: ${#UNITS[@]} unit(s) on $INSTANCES x $TYPE. Output: $OUT"
printf '%s\n' "${UNITS[@]}" > "$OUT/queue.txt"

# The working tree as git sees it (tracked + new untracked, minus ignored): uncommitted changes included,
# bin/obj and secrets left out.
log "Packing the working tree..."
(cd "$REPO" && git ls-files -co --exclude-standard -z | tar --null --force-local -czf "$OUT/repo.tgz" -T -) || { log "Packing failed."; exit 1; }

BDM_FILE="$OUT/bdm.json"
echo '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":60,"VolumeType":"gp3","DeleteOnTermination":true}}]' > "$BDM_FILE"
log "Launching $INSTANCES instance(s)..."
LAUNCHED=1
mapfile -t INSTANCE_IDS < <(aws_text ec2 run-instances \
    --image-id "$AMI_ID" --instance-type "$TYPE" --count "$INSTANCES" \
    --key-name "$KEY_NAME" --security-group-ids "$SECURITY_GROUP_ID" --subnet-id "$SUBNET_ID" \
    --instance-initiated-shutdown-behavior terminate \
    --block-device-mappings "file://$(cygpath -w "$BDM_FILE")" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$RUN_ID},{Key=ScenarioRun,Value=$RUN_ID}]" \
    --query "Instances[].InstanceId" | tr '\t' '\n')
if [ ${#INSTANCE_IDS[@]} -eq 0 ]; then log "No instances launched (see the error above)."; exit 1; fi
printf '%s\n' "${INSTANCE_IDS[@]}" > "$OUT/instances.txt"
aws ec2 wait instance-running --instance-ids "${INSTANCE_IDS[@]}"

# Queue: one unit per line; workers pop under a mkdir lock (atomic on every filesystem).
pop_unit() {
    until mkdir "$OUT/.queue.lock" 2>/dev/null; do sleep 0.2; done
    local next; next="$(head -n 1 "$OUT/queue.txt")"
    [ -n "$next" ] && sed -i '1d' "$OUT/queue.txt"
    rmdir "$OUT/.queue.lock"
    echo "$next"
}

worker() {
    local id="$1" host unit rc start
    host="$(aws_text ec2 describe-instances --instance-ids "$id" --query "Reservations[].Instances[].PublicDnsName")"
    if [ -z "$host" ]; then log "$id: no public DNS name; its units go to the other instances."; return; fi
    for _ in $(seq 1 60); do ssh "${SSH_OPTS[@]}" "ubuntu@$host" true 2>/dev/null && break; sleep 5; done

    log "$id ($host): setting up..."
    # A fresh instance can restart SSH right after its first successful login (cloud-init), so retry the upload.
    local uploaded=0
    for _ in 1 2 3 4 5; do
        scp "${SSH_OPTS[@]}" "$OUT/repo.tgz" "$HERE/setup-instance.sh" "$HERE/run-unit.sh" "ubuntu@$host:" >/dev/null 2>>"$OUT/ssh-$id.log" \
            && { uploaded=1; break; }
        sleep 15
    done
    if [ $uploaded -eq 0 ] \
        || ! ssh "${SSH_OPTS[@]}" "ubuntu@$host" "sed -i 's/\r\$//' setup-instance.sh run-unit.sh && bash setup-instance.sh $MAX_MINUTES" >"$OUT/setup-$id.log" 2>&1; then
        log "$id: setup FAILED (see setup-$id.log); its units go to the other instances."
        return
    fi
    log "$id: ready."

    while unit="$(pop_unit)"; [ -n "$unit" ]; do
        start=$(date +%s)
        log "$id: start $unit"
        # 'sg docker' gives the new docker group to this session without logging in again.
        rc="$(ssh "${SSH_OPTS[@]}" "ubuntu@$host" "sg docker -c 'bash run-unit.sh $unit $UNIT_TIMEOUT_MINUTES'" 2>>"$OUT/ssh-$id.log" | tail -n 1)"
        mkdir -p "$OUT/$unit"
        scp -r "${SSH_OPTS[@]}" "ubuntu@$host:results/$unit/*" "$OUT/$unit/" >/dev/null 2>&1
        [ -z "$rc" ] && rc="ssh-lost"
        local minutes=$(( ($(date +%s) - start) / 60 ))
        # Peak 1-min load average and used memory (GB) from the unit's load.log.
        local peak="load=? mem=?"
        [ -f "$OUT/$unit/load.log" ] && peak="$(awk '{ if ($2 > l) l = $2; if ($3 > m) m = $3 } END { printf "load=%.1f mem=%.1fG", l, m / 1024 }' "$OUT/$unit/load.log")"
        if [ "$rc" = "0" ]; then status="PASSED"; else status="FAILED($rc)"; echo "$id" >> "$OUT/failed-instances.txt"; fi
        echo "$status $unit ${minutes}m $peak $id" >> "$OUT/status.txt"
        log "$id: $status $unit (${minutes} min, $peak)"
    done
}

for id in "${INSTANCE_IDS[@]}"; do worker "$id" & done
wait

# Units no instance could take (every setup failed, or the run was cut short).
while unit="$(pop_unit)"; [ -n "$unit" ]; do echo "NOT-RUN $unit" >> "$OUT/status.txt"; done

{
    echo "Run $RUN_ID -- ${#UNITS[@]} unit(s) on $INSTANCES x $TYPE"
    sort "$OUT/status.txt" | column -t
    echo
    echo "Passed: $(grep -c '^PASSED' "$OUT/status.txt")  Failed: $(grep -c '^FAILED' "$OUT/status.txt")  Not run: $(grep -c '^NOT-RUN' "$OUT/status.txt")"
} | tee "$OUT/summary.txt"

grep -q -v '^PASSED' "$OUT/status.txt" && exit 1
exit 0
