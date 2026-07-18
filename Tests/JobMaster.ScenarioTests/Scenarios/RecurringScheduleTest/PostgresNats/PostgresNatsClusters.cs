namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest.PostgresNats;

// Member name -> kebab-cased ClusterId (max 25 chars) -- see ClusterConfigBuilder.ClusterId.
// Unlike the other RecurringScheduleTest variants, this one can't be standalone: NATS can only
// ever be an agent connection, never a cluster's own master storage (see
// ScheduleTest.PostgresNats). Postgres master + one NATS agent connection is enough here -- unlike
// ScheduleTest.PostgresNats this isn't testing connection/drain lifecycle, just recurring-firing
// correctness through a NATS-backed transport.
public enum PostgresNatsClusters
{
    PostgresNatsRecurring = 1 // "postgres-nats-recurring" (23 chars)
}
