using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ApiAuths.NoAuth;

public sealed class NoAuthPhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : AuthApiPhase1EmulatorBase<NoAuthClusters, NoAuthPhases>(global, runner)
{
    public override NoAuthPhases Phase() => NoAuthPhases.Phase1;
}
