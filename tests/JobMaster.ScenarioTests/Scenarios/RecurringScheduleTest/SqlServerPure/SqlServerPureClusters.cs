namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest.SqlServerPure;

// Member name -> kebab-cased ClusterId (max 25 chars) -- see ClusterConfigBuilder.ClusterId.
// A single standalone cluster is enough here -- see RecurringScheduleTest.PostgresPure for why.
public enum SqlServerPureClusters
{
    SqlserverRecurring = 1 // "sqlserver-recurring" (20 chars)
}
