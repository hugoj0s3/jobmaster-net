using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ApiAuths.ApiKey;

public sealed class ApiKeyPhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : AuthApiPhase1EmulatorBase<ApiKeyClusters, ApiKeyPhases>(global, runner)
{
    public override ApiKeyPhases Phase() => ApiKeyPhases.Phase1;
}
