using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest.PostgresNats;

public sealed class PostgresNatsTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<PostgresNatsScenarioEmulator, PostgresNatsClusters, PostgresNatsPhases>(global);
