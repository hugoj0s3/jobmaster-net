using System.Text;
using Npgsql;

namespace JobMaster.ScenarioTests.Scenarios.ArchivedModeTest.PostgresPure;

public sealed class PostgresPurePhase1Emulator(JobMaster.ScenarioTests.Fixtures.ScenarioGlobalEnvironment global, JobMaster.ScenarioTests.Runner.ScenarioRunner runner)
    : ArchivedModeTestPhase1EmulatorBase<PostgresPurePhases>(global, runner)
{
    protected override string SourceClusterId => "archive-source";
    protected override string TargetClusterId => "archive-target";
    protected override string ContainerName => "pg-archive";

    public override PostgresPurePhases Phase() => PostgresPurePhases.Phase1;

    // Timeout diagnostics: master rows referencing the missing ids, and every agent row.
    protected override async Task<string> DumpRawAsync(IReadOnlyCollection<Guid> missingIds)
    {
        var sb = new StringBuilder();
        var postgres = await Global.GetOrStartPostgresAsync();
        foreach (var (database, allRows) in new[] { ("ArchiveSource", false), ("ArchiveSourceAgent", true) })
        {
            sb.AppendLine($"#### {database}");
            var cs = new NpgsqlConnectionStringBuilder(postgres.GetConnectionString()) { Database = database }.ConnectionString;
            await using var cnn = new NpgsqlConnection(cs);
            await cnn.OpenAsync();

            var targets = new List<(string Table, string? Column)>();
            await using (var cmd = cnn.CreateCommand())
            {
                cmd.CommandText = allRows
                    ? "SELECT table_name, NULL FROM information_schema.tables WHERE table_schema = 'public'"
                    : "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'public' " +
                      "AND data_type = 'uuid' AND column_name IN ('id', 'job_id', 'reference_id')";
                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                    targets.Add((reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1)));
            }

            foreach (var (table, column) in targets)
            {
                await using var cmd = cnn.CreateCommand();
                cmd.CommandText = column == null
                    ? $"SELECT row_to_json(t)::text FROM \"{table}\" t LIMIT 50"
                    : $"SELECT row_to_json(t)::text FROM \"{table}\" t WHERE \"{column}\" = ANY(@ids)";
                if (column != null)
                    cmd.Parameters.AddWithValue("ids", missingIds.ToArray());
                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                    sb.AppendLine($"{table}.{column}: {reader.GetString(0)}");
            }
        }

        return sb.ToString();
    }
}
