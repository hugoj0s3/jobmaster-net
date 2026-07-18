using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest.PostgresNats;

public sealed class PostgresNatsPhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : RecurringScheduleTestPhase1EmulatorBase<PostgresNatsPhases>(global, runner)
{
    protected override string ClusterId => PostgresNatsClusters.PostgresNatsRecurring.ToString().ToKebabCase();

    // NatsJetStreamConstants.MaxThreshold caps TransientThreshold at 5 minutes for any cluster with
    // a NATS agent connection -- RecurringSchedulePlanner's planning horizon is max(TransientThreshold,
    // 5min), so on this cluster the horizon is *always* exactly 5 minutes, strictly less than the
    // 6-minute interval. The first occurrence can't be planned on the very first pass (candidate
    // exceeds the horizon) -- it needs real time to advance far enough that a *later* replanning
    // pass's horizon reaches the candidate date, which can genuinely push the first execution later
    // than the other (Postgres/MySql/SqlServer) variants, whose 10-minute TransientThreshold clears
    // the interval on the first pass. Both budgets below are widened accordingly.
    protected override TimeSpan WaitForTwoFiringsTimeout => TimeSpan.FromMinutes(24);
    protected override TimeSpan FirstFiringLateTolerance => TimeSpan.FromMinutes(9);

    public override PostgresNatsPhases Phase() => PostgresNatsPhases.Phase1;
}
