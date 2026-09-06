namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.SqlServerDist;

// Member name -> kebab-cased ClusterId (max 25 chars, letters/numbers/hyphens/underscores only --
// see ClusterConfigBuilder.ClusterId).
//
// Spelled "Sqlserver" (not "SqlServer") deliberately: StringUtil.ToKebabCase inserts a hyphen
// before every uppercase letter, so "SqlServerDrainLoad" would kebab-case to
// "sql-server-drain-load", not "sqlserver-drain-load". Keeping the product name as a single
// PascalCase word (no internal capital) is what makes ToKebabCase produce the right ClusterId --
// same reason "PostgresDrainLoad" works.
public enum SqlServerDistClusters
{
    SqlserverDrainLoad = 1 // "sqlserver-drain-load" (20 chars)
}
