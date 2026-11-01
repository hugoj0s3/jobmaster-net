namespace JobMaster.ScenarioTests.Scenarios.ArchivedModeTest.PostgresPure;

// Member names -> kebab-cased ClusterId (see ClusterConfigBuilder.ClusterId).
public enum PostgresPureClusters
{
    ArchiveSource = 1, // "archive-source"
    ArchiveTarget = 2, // "archive-target"
    StrayOld = 3,      // "stray-old": Active -> Migrating (phase 2) -> Archived (phase 3)
    StrayNew = 4       // "stray-new": stray-old's MigrateTo target
}
