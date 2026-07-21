using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest.Postgres;

public sealed class PostgresTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<PostgresScenarioEmulator, PostgresClusters, PostgresPhases>(global);
