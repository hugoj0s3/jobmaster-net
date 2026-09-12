using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.MigratingModeTest.MySqlPure;

public sealed class MySqlPureTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<MySqlPureScenarioEmulator, MySqlPureClusters, MySqlPurePhases>(global);
