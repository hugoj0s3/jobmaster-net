using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.PostgresDist;

public sealed class PostgresDistScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<PostgresDistClusters, PostgresDistPhases>(global);
