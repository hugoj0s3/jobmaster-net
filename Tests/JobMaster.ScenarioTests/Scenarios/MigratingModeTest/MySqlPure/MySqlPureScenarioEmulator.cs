using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.MigratingModeTest.MySqlPure;

public sealed class MySqlPureScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<MySqlPureClusters, MySqlPurePhases>(global);
