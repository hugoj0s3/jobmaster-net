using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ApiAuths.UserPass;

public sealed class UserPassPhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : AuthApiPhase1EmulatorBase<UserPassClusters, UserPassPhases>(global, runner)
{
    public override UserPassPhases Phase() => UserPassPhases.Phase1;
}
