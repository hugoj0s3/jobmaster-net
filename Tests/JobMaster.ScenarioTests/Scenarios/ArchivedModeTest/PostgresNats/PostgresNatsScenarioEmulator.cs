using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.ArchivedModeTest.PostgresNats;

public sealed class PostgresNatsScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<PostgresNatsClusters, PostgresNatsPhases>(global);
