namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.RavenDbPure;

// Member names -> kebab-cased ClusterId (max 25 chars, letters/numbers/hyphens/underscores only —
// see ClusterConfigBuilder.ClusterId). Spelled "Ravendb" (not "RavenDb") deliberately: ToKebabCase
// inserts a hyphen before every uppercase letter after the first, so "RavenDbStandalone" would kebab
// to the wrong "raven-db-standalone" (3 segments) instead of "ravendb-standalone" (2 segments).
public enum RavenDbPureClusters
{
    RavendbStandalone = 1, // "ravendb-standalone" (19 chars)
    RavendbDistOne = 2,    // "ravendb-dist-one" (16 chars)
    RavendbDistTwo = 3     // "ravendb-dist-two" (16 chars)
}
