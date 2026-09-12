using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.ApiAuths.Jwt;

public sealed class JwtTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<JwtScenarioEmulator, JwtClusters, JwtPhases>(global);
