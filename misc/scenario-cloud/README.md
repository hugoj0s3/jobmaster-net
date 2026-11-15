# Scenario tests on EC2

Runs `tests/JobMaster.ScenarioTests` on fresh EC2 instances in parallel, from the local working tree
(uncommitted changes included), and terminates them at the end.

## Setup (once)
- AWS CLI configured (`aws configure`), Git Bash.
- Copy `scenario-cloud.settings.example.sh` to `scenario-cloud.settings.sh` (gitignored) and fill in the AMI
  (Ubuntu x86_64), key pair, `.pem` path, security group (SSH from this PC) and a public subnet.

## Run (Git Bash, repo root)
```bash
misc/scenario-cloud/run-scenarios.sh --list                         # units, longest first; no AWS calls
misc/scenario-cloud/run-scenarios.sh -n 2 -u 'BasicExecution|ApiAuths.NoAuth'   # small dry run
misc/scenario-cloud/run-scenarios.sh                                # everything, 10 x m5.2xlarge
```
- A unit is one scenario variant, `<Family>.<Variant>` (e.g. `ScheduleTest.PostgresPure`), taken from the
  `Scenarios/` folders. Free instances take the next unit, longest first.
- Results: `artifacts/scenario-runs/<run-id>/` — `summary.txt`, `run.log`, and per unit the `.trx`,
  `console.log` and timeout dumps (`diag/`).
- `--keep-failed` leaves the instances of failed units running for SSH (`ssh -i <pem> ubuntu@<dns>`).

## Cost safety
- Instances are terminated when the script ends (also on errors and Ctrl+C).
- Each one also shuts itself down after `MAX_MINUTES` (330) and is launched with shutdown behavior
  "terminate", so a crashed script can't leave it billing.
- Leftovers check: `aws ec2 describe-instances --filters Name=tag-key,Values=ScenarioRun Name=instance-state-name,Values=pending,running --query "Reservations[].Instances[].[InstanceId,Tags[?Key=='ScenarioRun']|[0].Value]" --output table`
