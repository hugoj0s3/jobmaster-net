using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.ApiAuths.Jwt;

public sealed class JwtScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<JwtClusters, JwtPhases>(global);
