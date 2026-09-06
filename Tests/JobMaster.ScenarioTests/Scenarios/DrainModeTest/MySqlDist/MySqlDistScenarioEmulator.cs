using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.MySqlDist;

public sealed class MySqlDistScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<MySqlDistClusters, MySqlDistPhases>(global);
