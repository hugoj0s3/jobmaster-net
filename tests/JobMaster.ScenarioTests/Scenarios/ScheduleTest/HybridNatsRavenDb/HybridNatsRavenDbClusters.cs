namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.HybridNatsRavenDb;

// Member names -> kebab-cased ClusterId (max 25 chars). RavenDB master; each cluster has two hybrid
// agent connections, NATS -> RavenDB and RavenDB -> NATS. No standalone cluster: NATS can't be a master.
public enum HybridNatsRavenDbClusters
{
    HybridDistOne = 1, // "hybrid-dist-one"
    HybridDistTwo = 2  // "hybrid-dist-two"
}
