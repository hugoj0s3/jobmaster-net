using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.MigratingModeTest.PostgresPure;

public sealed class PostgresPureTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<PostgresPureScenarioEmulator, PostgresPureClusters, PostgresPurePhases>(global);
