namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.PostgresDist;

// Member name -> kebab-cased ClusterId (max 25 chars, letters/numbers/hyphens/underscores only --
// see ClusterConfigBuilder.ClusterId).
public enum PostgresDistClusters
{
    PostgresDrainLoad = 1 // "postgres-drain-load" (20 chars)
}
