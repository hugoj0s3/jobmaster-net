namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.PostgresNats;

// Member names -> kebab-cased ClusterId (max 25 chars, letters/numbers/hyphens/underscores only —
// see ClusterConfigBuilder.ClusterId). No standalone cluster here, unlike the *Pure scenarios:
// NatsJetStreamConnectionOptionsStrategy.SetOptions(IClusterConfigSelector, ...) throws for master
// usage ("NATS JetStream is not supported as a cluster master repository"), so a NATS-backed
// cluster can only ever be a distributed cluster (Postgres master + NATS agent connections), never
// a standalone one.
public enum PostgresNatsClusters
{
    PostgresNatsDistOne = 1, // "postgres-nats-dist-one" (22 chars)
    PostgresNatsDistTwo = 2  // "postgres-nats-dist-two" (22 chars)
}
