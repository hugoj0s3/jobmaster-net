namespace JobMaster.ScenarioTests.Scenarios.MigratingModeTest.PostgresNats;

// Member names -> kebab-cased ClusterId (see ClusterConfigBuilder.ClusterId).
public enum PostgresNatsClusters
{
    MigratingSource = 1, // "migrating-source"
    MigratingTarget = 2  // "migrating-target"
}
