using FluentAssertions;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;
using Npgsql;

namespace JobMaster.ScenarioTests.Scenarios.ArchivedModeTest.PostgresPure;

/// <summary>
/// Leaves late work on an archived cluster, the way the scheduler's non-atomic mode check can: stray-old
/// is Migrating (MigrateTo stray-new) with no Coordinator, so the jobs and the schedule written here stay
/// on it. Then the stored mode is switched to Archived directly in the database -- i.e. the switch landed
/// right after those writes passed the mode check. Phase3 restarts stray-old as Archived.
/// </summary>
public sealed class PostgresPurePhase2Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<PostgresPurePhases>(global, runner)
{
    private const string ContainerName = "pg-stray";
    private const string StrayOldClusterId = "stray-old";
    private const string StrayOldDatabase = "StrayOld";
    private const int QtyJobs = 20;
    private const int ArchivedMode = 3;

    public override PostgresPurePhases Phase() => PostgresPurePhases.Phase2;

    public override async Task RunAsync()
    {
        var api = Runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");

        // Phase1's clusters are unrelated to this flow.
        await Runner.StopAsync("pg-archive");

        PostgresPureState.TestIdentifier = Guid.NewGuid().ToString("N");
        PostgresPureState.RecurringTestIdentifier = Guid.NewGuid().ToString("N");

        var scheduled = await Runner.ScheduleFor(ContainerName)
            .ScheduleAsync("fast", PostgresPureState.TestIdentifier, QtyJobs, clusterId: StrayOldClusterId);
        scheduled.JobIds.Should().HaveCount(QtyJobs);
        PostgresPureState.LateJobIds = scheduled.JobIds.ToList();

        var recurring = await Runner.RecurringScheduleFor(ContainerName)
            .CreateRecurringAsync("fast", "TimeSpanInterval", "00:00:15", PostgresPureState.RecurringTestIdentifier, clusterId: StrayOldClusterId);
        PostgresPureState.LateRecurringScheduleId = recurring.RecurringScheduleId;

        (await api.GetJobCountAsync(StrayOldClusterId, testIdentifier: PostgresPureState.TestIdentifier)).Should().Be(QtyJobs);
        (await api.GetRecurringSchedulesAsync(StrayOldClusterId, PostgresPureState.RecurringTestIdentifier)).Should().ContainSingle();

        await Runner.StopAsync(ContainerName);

        var postgres = await Global.GetOrStartPostgresAsync();
        var connectionString = new NpgsqlConnectionStringBuilder(postgres.GetConnectionString()) { Database = StrayOldDatabase }.ConnectionString;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            "UPDATE jm_generic_record_entry_value v SET value_int64 = @mode " +
            "FROM jm_generic_record_entry e " +
            "WHERE v.record_unique_id = e.record_unique_id AND e.group_id = 'ClusterConfiguration' " +
            "AND e.entry_id = @clusterId AND v.key_name = 'ClusterMode'";
        command.Parameters.AddWithValue("mode", (long)ArchivedMode);
        command.Parameters.AddWithValue("clusterId", StrayOldClusterId);
        (await command.ExecuteNonQueryAsync()).Should().Be(1, "stray-old's stored ClusterMode must be switched to Archived");
    }
}
