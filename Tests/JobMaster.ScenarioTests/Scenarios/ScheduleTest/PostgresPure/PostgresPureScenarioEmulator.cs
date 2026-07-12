using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.PostgresPure;

public sealed class PostgresPureScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<PostgresPureClusters, PostgresPurePhases>(global);
