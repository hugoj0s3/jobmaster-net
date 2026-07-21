using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.ArchivedModeTest.MySqlPure;

public sealed class MySqlPureTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<MySqlPureScenarioEmulator, MySqlPureClusters, MySqlPurePhases>(global);
