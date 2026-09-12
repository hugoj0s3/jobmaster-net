using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.PostgresDist;

public sealed class PostgresDistTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<PostgresDistScenarioEmulator, PostgresDistClusters, PostgresDistPhases>(global);
