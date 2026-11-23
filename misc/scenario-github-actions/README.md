# Scenario tests on GitHub Actions

Runs the scenario flavor matrix (`tests/JobMaster.ScenarioMatrixTests`) on GitHub-hosted runners, one runner per unit,
from the **pushed commit**. To run uncommitted changes, use `misc/scenario-aws` instead.

## Run
Actions → **Scenario Tests** → Run workflow, pick the branch, optionally a `units` regex. Or from a terminal:
```bash
gh workflow run schedule-scenario-tests.yml --ref <branch>                          # everything
gh workflow run schedule-scenario-tests.yml --ref <branch> -f units='DrainModeTest'  # one scenario, every variant
gh workflow run schedule-scenario-tests.yml --ref <branch> -f units='BasicExecution|ApiAuthTest'
```
- A unit is one theory row of the matrix, `<Scenario>.<Flavor>[.<StopType|Auth>]` (e.g. `ScheduleTest.PostgresPure`,
  `DrainModeTest.MySqlPure.Crash`), the same units as `misc/scenario-aws`. List them locally (Git Bash):
  `bash misc/scenario-github-actions/list-units.sh 'DrainModeTest'`.
- Results: one artifact per unit (`.trx`, `console.log`, `load.log`, timeout dumps in `diag/`), kept 14 days. The
  unit's console output is also in its job log, ending with the peak load and memory.
- `All Scenario Tests Passed` is the single check to require in branch protection.

## Limits
- Free on a public repo, standard runners (4 vCPU, 16 GB): at most 20 jobs at once, so the 82 units queue; the
  longest start first. ScheduleTest (~65–75 min) is the floor.
- Each job builds the project and the app images itself (a few minutes on top of the unit).
