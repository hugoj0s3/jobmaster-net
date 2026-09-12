using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.MySqlPure;

public sealed class MySqlPureScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<MySqlPureClusters, MySqlPurePhases>(global);
