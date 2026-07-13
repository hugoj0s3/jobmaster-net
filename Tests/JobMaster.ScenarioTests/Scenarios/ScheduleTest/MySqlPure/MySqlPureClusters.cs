namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.MySqlPure;

// Member names -> kebab-cased ClusterId (max 25 chars, letters/numbers/hyphens/underscores only —
// see ClusterConfigBuilder.ClusterId). Deliberately short: "Pure"/"Cluster" are dropped since the
// MySqlPure scenario folder already gives that context.
//
// Spelled "Mysql" (not "MySql") deliberately: StringUtil.ToKebabCase inserts a hyphen before every
// uppercase letter, so "MySqlStandalone" would kebab-case to "my-sql-standalone", not
// "mysql-standalone". Keeping the product name as a single PascalCase word (no internal capital)
// is what makes ToKebabCase produce the right ClusterId — same reason "PostgresStandalone" works.
public enum MySqlPureClusters
{
    MysqlStandalone = 1, // "mysql-standalone" (16 chars)
    MysqlDistOne = 2,    // "mysql-dist-one" (14 chars)
    MysqlDistTwo = 3     // "mysql-dist-two" (14 chars)
}
