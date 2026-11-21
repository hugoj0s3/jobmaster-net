using JobMaster.ScenarioMatrixTests.Lib;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;

namespace JobMaster.ScenarioMatrixTests.Flavors;

/// <summary>
/// Direct writes to a flavor's master database, for scenarios that need a stored state the API can't produce (e.g. a
/// mode switch that landed between two writes). Per master provider, so <see cref="ScenarioFlavor"/> exposes them.
/// </summary>
internal static class MasterStoreRaw
{
    // The cluster's stored ClusterConfiguration record: generic-record group "ClusterConfiguration", entry id = the
    // cluster id, value "ClusterMode" (an int64 in the SQL value table, a number in the RavenDB document's Values).
    private const string SqlServerMySqlFilter =
        "e.group_id = 'ClusterConfiguration' AND e.entry_id = @clusterId AND v.key_name = 'ClusterMode'";

    // The default table prefix is "JM_" (SqlJobMasterRuntimeSetup). Postgres folds unquoted names to lower case and
    // SQL Server's default collation ignores case, but MySQL on Linux doesn't: the exact casing is required there.
    private const string EntryTable = "JM_generic_record_entry";
    private const string ValueTable = "JM_generic_record_entry_value";

    public static async Task<int> SetStoredClusterModeAsync(
        ScenarioGlobalEnvironment global, string masterRepoType, string database, string clusterId, int mode)
    {
        switch (masterRepoType)
        {
            case "Postgres":
            {
                var postgres = await global.GetOrStartPostgresAsync();
                var cs = new NpgsqlConnectionStringBuilder(postgres.GetConnectionString()) { Database = database }.ConnectionString;
                await using var connection = new NpgsqlConnection(cs);
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"UPDATE {ValueTable} v SET value_int64 = @mode " +
                    $"FROM {EntryTable} e " +
                    "WHERE v.record_unique_id = e.record_unique_id AND e.group_id = 'ClusterConfiguration' " +
                    "AND e.entry_id = @clusterId AND v.key_name = 'ClusterMode'";
                command.Parameters.AddWithValue("mode", (long)mode);
                command.Parameters.AddWithValue("clusterId", clusterId);
                return await command.ExecuteNonQueryAsync();
            }
            case "MySql":
            {
                var mySql = await global.GetOrStartMySqlAsync();
                var cs = new MySqlConnectionStringBuilder(mySql.GetConnectionString()) { Database = database }.ConnectionString;
                await using var connection = new MySqlConnection(cs);
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"UPDATE {ValueTable} v JOIN {EntryTable} e ON v.record_unique_id = e.record_unique_id " +
                    $"SET v.value_int64 = @mode WHERE {SqlServerMySqlFilter}";
                command.Parameters.AddWithValue("@mode", (long)mode);
                command.Parameters.AddWithValue("@clusterId", clusterId);
                return await command.ExecuteNonQueryAsync();
            }
            case "SqlServer":
            {
                var sqlServer = await global.GetOrStartSqlServerAsync();
                var cs = new SqlConnectionStringBuilder(sqlServer.GetConnectionString()) { InitialCatalog = database }.ConnectionString;
                await using var connection = new SqlConnection(cs);
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"UPDATE v SET v.value_int64 = @mode FROM {ValueTable} v " +
                    $"JOIN {EntryTable} e ON v.record_unique_id = e.record_unique_id WHERE {SqlServerMySqlFilter}";
                command.Parameters.AddWithValue("@mode", (long)mode);
                command.Parameters.AddWithValue("@clusterId", clusterId);
                return await command.ExecuteNonQueryAsync();
            }
            case "RavenDB":
            {
                var ravenDb = await global.GetOrStartRavenDbAsync();
                using var store = new DocumentStore { Urls = [ravenDb.GetConnectionString()], Database = database };
                store.Initialize();
                // RavenDbDocumentNaming: {prefix}{collection}/{clusterId}/{entryId}, default prefix "JM_".
                var documentId = $"JM_ClusterConfiguration/{clusterId}/{clusterId}";
                var status = await store.Operations.SendAsync(new PatchOperation(documentId, null, new PatchRequest
                {
                    Script = "this.Values.ClusterMode = args.mode;",
                    Values = { ["mode"] = (long)mode }
                }));
                return status == PatchStatus.Patched ? 1 : 0;
            }
            default:
                throw new NotSupportedException($"No raw master access for repo type '{masterRepoType}'.");
        }
    }
}
