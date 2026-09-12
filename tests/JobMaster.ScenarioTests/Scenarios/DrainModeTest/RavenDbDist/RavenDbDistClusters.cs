namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.RavenDbDist;

// Member name -> kebab-cased ClusterId (max 25 chars, letters/numbers/hyphens/underscores only --
// see ClusterConfigBuilder.ClusterId). Single PascalCase word "Ravendb" (not "RavenDb") so
// StringUtil.ToKebabCase() produces "ravendb-..." instead of wrongly splitting into "raven-db-...".
public enum RavenDbDistClusters
{
    RavendbDrainLoad = 1 // "ravendb-drain-load" (19 chars)
}
