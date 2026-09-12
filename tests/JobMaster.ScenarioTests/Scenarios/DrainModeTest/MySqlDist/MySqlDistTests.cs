using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.MySqlDist;

public sealed class MySqlDistTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<MySqlDistScenarioEmulator, MySqlDistClusters, MySqlDistPhases>(global);
