namespace JobMaster.Postgres;

internal static class PostgresRepositoryConstants
{
    public const string RepositoryTypeId = "Postgres";
    public const string CaseInsensitiveCollation = "jm_28ab036805db4d59b2147df6f949a5de_ci";
    
    internal const string CreateCaseInsensitiveCollationSql = $@"
                CREATE COLLATION IF NOT EXISTS {CaseInsensitiveCollation} (
                    provider = icu,
                    locale = 'und-u-ks-level2',
                    deterministic = false
                );";
}
