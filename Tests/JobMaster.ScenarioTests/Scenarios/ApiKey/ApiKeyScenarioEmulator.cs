using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.ApiAuths.ApiKey;

public sealed class ApiKeyScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<ApiKeyClusters, ApiKeyPhases>(global);
