namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.SqlServerPure;

// Member names -> kebab-cased ClusterId (max 25 chars, letters/numbers/hyphens/underscores only —
// see ClusterConfigBuilder.ClusterId). Deliberately short: "Pure"/"Cluster" are dropped since the
// SqlServerPure scenario folder already gives that context.
//
// Spelled "Sqlserver" (not "SqlServer") deliberately: StringUtil.ToKebabCase inserts a hyphen
// before every uppercase letter, so "SqlServerStandalone" would kebab-case to
// "sql-server-standalone", not "sqlserver-standalone". Keeping the product name as a single
// PascalCase word (no internal capital) is what makes ToKebabCase produce the right ClusterId —
// same reason "PostgresStandalone" works.
public enum SqlServerPureClusters
{
    SqlserverStandalone = 1, // "sqlserver-standalone" (20 chars)
    SqlserverDistOne = 2,    // "sqlserver-dist-one" (18 chars)
    SqlserverDistTwo = 3     // "sqlserver-dist-two" (18 chars)
}
