using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.ApiAuths.ApiKey;

public sealed class ApiKeyTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<ApiKeyScenarioEmulator, ApiKeyClusters, ApiKeyPhases>(global);
