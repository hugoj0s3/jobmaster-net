using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest.MySqlPure;

public sealed class MySqlPureTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<MySqlPureScenarioEmulator, MySqlPureClusters, MySqlPurePhases>(global);
