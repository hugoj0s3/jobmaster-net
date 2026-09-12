namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest.PostgresPure;

// Member name -> kebab-cased ClusterId (max 25 chars) -- see ClusterConfigBuilder.ClusterId.
// A single standalone cluster is enough here: unlike ScheduleTest's Pure scenarios, this isn't
// testing connection/drain lifecycle, just recurring-schedule firing correctness.
public enum PostgresPureClusters
{
    PostgresRecurring = 1 // "postgres-recurring" (19 chars)
}
