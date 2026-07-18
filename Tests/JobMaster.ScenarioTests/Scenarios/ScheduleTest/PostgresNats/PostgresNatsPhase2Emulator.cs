using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.PostgresNats;

public sealed class PostgresNatsPhase2Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : DataRetentionPhase2EmulatorBase<PostgresNatsPhases>(global, runner)
{
    // No TtlOnlyClusterId override: TTL purge is pure master-side logic, already proven by
    // PostgresPure/MySqlPure/SqlServerPure. This phase exists purely to exercise NATS's own
    // drain-runner implementations (NatsJetStreamDrainSavePendingJobsRunner etc.), which nothing
    // else in the codebase currently covers against real bucket-status transitions.
    protected override string DrainClusterId => PostgresNatsClusters.PostgresNatsDistOne.ToString().ToKebabCase();
    protected override string ControlClusterId => PostgresNatsClusters.PostgresNatsDistTwo.ToString().ToKebabCase();

    public override PostgresNatsPhases Phase() => PostgresNatsPhases.Phase2;
}
