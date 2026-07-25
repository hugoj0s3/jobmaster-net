using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest.MySqlPure;

public sealed class MySqlPureTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<MySqlPureScenarioEmulator, MySqlPureClusters, MySqlPurePhases>(global);
