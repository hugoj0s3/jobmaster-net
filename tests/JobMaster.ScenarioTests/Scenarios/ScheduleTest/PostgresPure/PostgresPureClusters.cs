namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.PostgresPure;

// Member names -> kebab-cased ClusterId (max 25 chars, letters/numbers/hyphens/underscores only —
// see ClusterConfigBuilder.ClusterId). Deliberately short: "Pure"/"Cluster" are dropped since the
// PostgresPure scenario folder already gives that context.
public enum PostgresPureClusters
{
    PostgresStandalone = 1, // "postgres-standalone" (19 chars)
    PostgresDistOne = 2,    // "postgres-dist-one" (17 chars)
    PostgresDistTwo = 3     // "postgres-dist-two" (17 chars)
}
