# Scenario tests on EC2

Runs the scenario flavor matrix (`tests/JobMaster.ScenarioMatrixTests`) on fresh EC2 instances in parallel, from the
local working tree (uncommitted changes included), and terminates them at the end. For the pushed commit on
GitHub-hosted runners, see `misc/scenario-github-actions`.

## Setup (once)
- AWS CLI configured (`aws configure`), Git Bash, .NET SDK (the unit list comes from a local build).
- Copy `scenario-aws.settings.example.sh` to `scenario-aws.settings.sh` (gitignored) and fill in the AMI
  (Ubuntu x86_64), key pair, `.pem` path, security group (SSH from this PC) and a public subnet.

## Run (Git Bash, repo root)
```bash
misc/scenario-aws/run-scenarios.sh --list                                  # units, longest first; no AWS calls
misc/scenario-aws/run-scenarios.sh -n 2 -u 'BasicExecution|ApiAuthTest'    # small dry run
misc/scenario-aws/run-scenarios.sh -u 'DrainModeTest'                      # one scenario, every variant
misc/scenario-aws/run-scenarios.sh                                         # everything, 10 x m5.2xlarge
```
- A unit is one theory row of the matrix, `<Scenario>.<Flavor>[.<StopType|Auth>]` (e.g. `ScheduleTest.PostgresPure`,
  `DrainModeTest.MySqlPure.Crash`, `ApiAuthTest.PostgresPure.jwt`). Free instances take the next unit, longest first.
- Results: `artifacts/scenario-runs/<run-id>/` — `summary.txt`, `run.log`, and per unit the `.trx`, `console.log`,
  `load.log` and timeout dumps (`diag/`).
- `--keep-failed` leaves the instances of failed units running for SSH (`ssh -i <pem> ubuntu@<dns>`).
- The whole matrix (82 units) took 1 h 38 min on 10 x m5.2xlarge on 2026-11-21; ScheduleTest (~65 min) is the floor.

## Cost safety
- Instances are terminated when the script ends (also on errors and Ctrl+C).
- Each one also shuts itself down after `MAX_MINUTES` (330) and is launched with shutdown behavior
  "terminate", so a crashed script can't leave it billing.
- Leftovers check: `aws ec2 describe-instances --filters Name=tag-key,Values=ScenarioRun Name=instance-state-name,Values=pending,running --query "Reservations[].Instances[].[InstanceId,Tags[?Key=='ScenarioRun']|[0].Value]" --output table`
