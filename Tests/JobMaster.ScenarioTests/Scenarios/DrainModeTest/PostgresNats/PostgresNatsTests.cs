using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.PostgresNats;

public sealed class PostgresNatsTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<PostgresNatsScenarioEmulator, PostgresNatsClusters, PostgresNatsPhases>(global);
