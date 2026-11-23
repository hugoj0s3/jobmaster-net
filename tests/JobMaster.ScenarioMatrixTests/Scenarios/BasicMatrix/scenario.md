# BasicMatrix

Prototype of the flavor matrix: one provider-free scenario, run on every flavor as an xUnit Theory.

## Flow
1. Start `api` and one distributed cluster `mx-<flavor>-basic` (3 agent connections, one worker each).
2. Schedule 30 `fast` jobs.
3. Every job runs exactly once (Redis tracker, re-read after a 5 s settle window), and the API shows the
   first job's execution.

## Flavors
| Flavor | Code | Master | Agent slots |
|---|---|---|---|
| PostgresPure | `pg` | Postgres | Postgres, own database per slot |
| MySqlPure | `my` | MySQL | MySQL, own database per slot |
| SqlServerPure | `ss` | SQL Server | SQL Server, own database per slot |
| RavenDbPure | `rv` | RavenDB | RavenDB, own database per slot |
| PostgresNats | `pgnats` | Postgres | NATS JetStream |
| HybridNatsRavenDb | `hyb` | RavenDB | odd slots NATS → RavenDB, even slots RavenDB → NATS |

## Template placeholders
- `{{flavor}}`: the flavor's code, in container names, aliases and cluster ids (`mx-` prefix keeps them apart
  from the regular scenario suite).
- `{{master:RepoType}}`, `{{master:ConnectionString:<db>}}`: the master provider; the database is named
  `Mx<Code><db>`.
- `{ "Name": "agent-1", "Connection": "{{agent:1}}" }`: `Connection` is replaced by the flavor's fields for
  that slot; the name stays.

Run one flavor: `dotnet test tests/JobMaster.ScenarioMatrixTests --filter "DisplayName~PostgresNats"`.
