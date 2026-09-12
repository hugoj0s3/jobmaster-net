using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.PostgresNats;

public sealed class PostgresNatsScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<PostgresNatsClusters, PostgresNatsPhases>(global);
