using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.ApiAuths.UserPass;

public sealed class UserPassScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<UserPassClusters, UserPassPhases>(global);
