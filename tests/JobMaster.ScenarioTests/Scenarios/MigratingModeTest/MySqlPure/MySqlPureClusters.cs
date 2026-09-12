namespace JobMaster.ScenarioTests.Scenarios.MigratingModeTest.MySqlPure;

// Member names -> kebab-cased ClusterId (see ClusterConfigBuilder.ClusterId).
public enum MySqlPureClusters
{
    MigratingSource = 1, // "migrating-source"
    MigratingTarget = 2  // "migrating-target"
}
