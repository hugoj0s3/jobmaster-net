namespace JobMaster.ScenarioTests.Scenarios.MigratingModeTest.PostgresPure;

// Member names -> kebab-cased ClusterId (see ClusterConfigBuilder.ClusterId).
public enum PostgresPureClusters
{
    MigratingSource = 1, // "migrating-source"
    MigratingTarget = 2  // "migrating-target"
}
