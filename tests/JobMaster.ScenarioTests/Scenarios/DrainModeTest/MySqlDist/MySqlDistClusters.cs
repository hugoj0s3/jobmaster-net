namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.MySqlDist;

// Member name -> kebab-cased ClusterId (max 25 chars, letters/numbers/hyphens/underscores only --
// see ClusterConfigBuilder.ClusterId).
//
// Spelled "Mysql" (not "MySql") deliberately: StringUtil.ToKebabCase inserts a hyphen before every
// uppercase letter, so "MySqlDrainLoad" would kebab-case to "my-sql-drain-load", not
// "mysql-drain-load". Keeping the product name as a single PascalCase word (no internal capital)
// is what makes ToKebabCase produce the right ClusterId -- same reason "PostgresDrainLoad" works.
public enum MySqlDistClusters
{
    MysqlDrainLoad = 1 // "mysql-drain-load" (17 chars)
}
