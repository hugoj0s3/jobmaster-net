# RecurringScheduleTest.SqlServerPure

Same as `RecurringScheduleTest.PostgresPure` (see that scenario's `scenario.md` for full rationale
— interval choice, `TransientThreshold` gotcha, compiler timing differences, assertions), just
against a standalone SQL Server cluster (`sqlserver-recurring`, database `SqlServerRecurring`)
instead of Postgres. `TargetTestRecurringApp` now references all 4 providers
(Postgres/MySql/SqlServer/NATS) the same way `TargetTestScheduleApp` does, so the same image serves
every recurring-schedule scenario regardless of provider.
