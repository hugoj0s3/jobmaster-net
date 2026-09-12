using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.PostgresNats;

public sealed class PostgresNatsScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<PostgresNatsClusters, PostgresNatsPhases>(global);
