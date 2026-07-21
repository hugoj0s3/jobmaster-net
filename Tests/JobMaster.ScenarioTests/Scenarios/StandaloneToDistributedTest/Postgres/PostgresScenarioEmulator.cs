using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest.Postgres;

public sealed class PostgresScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<PostgresClusters, PostgresPhases>(global);
