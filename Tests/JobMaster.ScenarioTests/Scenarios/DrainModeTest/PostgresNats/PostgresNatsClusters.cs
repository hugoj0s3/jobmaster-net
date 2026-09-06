namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.PostgresNats;

// Member name -> kebab-cased ClusterId (max 25 chars, letters/numbers/hyphens/underscores only --
// see ClusterConfigBuilder.ClusterId). No naming gotcha here (unlike RavenDb/MySql/SqlServer): both
// "Postgres" and "Nats" are already single PascalCase words with no internal capital, so
// StringUtil.ToKebabCase splits them into two separate kebab segments exactly as intended.
public enum PostgresNatsClusters
{
    PostgresNatsDrainLoad = 1 // "postgres-nats-drain-load" (24 chars)
}
