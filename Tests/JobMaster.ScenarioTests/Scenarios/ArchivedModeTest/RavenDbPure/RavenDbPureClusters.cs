namespace JobMaster.ScenarioTests.Scenarios.ArchivedModeTest.RavenDbPure;

// Member names -> kebab-cased ClusterId (see ClusterConfigBuilder.ClusterId). Unlike the RavenDbPure
// variants elsewhere in this suite, no "Ravendb"-vs-"RavenDb" spelling concern applies here: these
// member names don't embed the provider name at all -- the topology (source/target) is what's being
// modeled, and the ClusterId text is intentionally identical across every repo-type variant of this
// scenario.
public enum RavenDbPureClusters
{
    ArchiveSource = 1, // "archive-source"
    ArchiveTarget = 2  // "archive-target"
}
