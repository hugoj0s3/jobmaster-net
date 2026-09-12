namespace JobMaster.ScenarioTests.Scenarios.MigratingModeTest.SqlServerPure;

// Member names -> kebab-cased ClusterId (see ClusterConfigBuilder.ClusterId).
public enum SqlServerPureClusters
{
    MigratingSource = 1, // "migrating-source"
    MigratingTarget = 2  // "migrating-target"
}
