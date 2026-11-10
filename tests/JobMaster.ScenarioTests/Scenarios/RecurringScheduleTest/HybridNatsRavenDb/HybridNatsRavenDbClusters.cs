namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest.HybridNatsRavenDb;

// Member name -> kebab-cased ClusterId (max 25 chars) -- see ClusterConfigBuilder.ClusterId.
// RavenDB master + two hybrid agent connections (NATS -> RavenDB and RavenDB -> NATS), so recurring
// occurrences are dispatched through both shapes. Not standalone: NATS can't be a master.
public enum HybridNatsRavenDbClusters
{
    HybridRecurring = 1 // "hybrid-recurring" (16 chars)
}
