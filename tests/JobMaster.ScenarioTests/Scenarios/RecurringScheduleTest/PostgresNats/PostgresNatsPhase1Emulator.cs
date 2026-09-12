using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest.PostgresNats;

public sealed class PostgresNatsPhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : RecurringScheduleTestPhase1EmulatorBase<PostgresNatsPhases>(global, runner)
{
    protected override string ClusterId => PostgresNatsClusters.PostgresNatsRecurring.ToString().ToKebabCase();

    // NatsJetStreamConstants.MaxThreshold caps TransientThreshold at 5 minutes for any cluster with a
    // NATS agent connection -- strictly less than this test's 6-minute interval. That used to force an
    // extra replanning cycle (and a much wider tolerance) before the first occurrence materialized, but
    // RecurringSchedulePlanner now always dispatches a schedule's next occurrence on its very first
    // planning attempt regardless of whether it fits within the horizon, so no override is needed here
    // any more -- this cluster behaves the same as the Pure variants.

    public override PostgresNatsPhases Phase() => PostgresNatsPhases.Phase1;
}
