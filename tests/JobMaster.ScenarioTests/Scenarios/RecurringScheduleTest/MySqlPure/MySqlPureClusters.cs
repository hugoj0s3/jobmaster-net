namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest.MySqlPure;

// Member name -> kebab-cased ClusterId (max 25 chars) -- see ClusterConfigBuilder.ClusterId.
// A single standalone cluster is enough here -- see RecurringScheduleTest.PostgresPure for why.
public enum MySqlPureClusters
{
    MysqlRecurring = 1 // "mysql-recurring" (15 chars)
}
