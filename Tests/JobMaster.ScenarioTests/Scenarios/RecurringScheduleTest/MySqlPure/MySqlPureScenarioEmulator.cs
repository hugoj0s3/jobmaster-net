using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest.MySqlPure;

public sealed class MySqlPureScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<MySqlPureClusters, MySqlPurePhases>(global);
