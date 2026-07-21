using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.ArchivedModeTest.MySqlPure;

public sealed class MySqlPureScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<MySqlPureClusters, MySqlPurePhases>(global);
