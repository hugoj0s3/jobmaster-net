# RecurringScheduleTest.HybridNatsRavenDb

Same as `RecurringScheduleTest.PostgresPure` (see that scenario's `scenario.md` for the base
rationale — interval choice, compiler timing, assertions), but on a RavenDB master with two
**hybrid** agent connections, each with its own worker:

- `hyb-nats-raven` (worker `w1`): transport NATS JetStream, execution RavenDB (`HybridRecurringExec1`).
- `hyb-raven-nats` (worker `w2`): transport RavenDB (`HybridRecurringTransport2`), execution NATS JetStream.

Recurring occurrences are spread across the buckets of both connections, so the planner's dispatch
goes through NATS → RavenDB and RavenDB → NATS in the same run.

Not standalone: NATS can't be a master. `TransientThreshold` is `00:05:00` because
`NatsJetStreamJobMasterRuntimeSetup` caps it at 5 minutes when NATS is on the execution side
(`hyb-raven-nats`), same as `RecurringScheduleTest.PostgresNats`.
