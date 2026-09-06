namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest.RavenDbPure;

// Member name -> kebab-cased ClusterId (max 25 chars) -- see ClusterConfigBuilder.ClusterId.
// A single standalone cluster is enough here: unlike ScheduleTest's Pure scenarios, this isn't
// testing connection/drain lifecycle, just recurring-schedule firing correctness.
// Spelled "Ravendb" (not "RavenDb") deliberately: ToKebabCase inserts a hyphen before every
// uppercase letter after the first, so "RavenDbRecurring" would kebab to the wrong
// "raven-db-recurring" (3 segments) instead of "ravendb-recurring" (2 segments).
public enum RavenDbPureClusters
{
    RavendbRecurring = 1 // "ravendb-recurring" (17 chars)
}
