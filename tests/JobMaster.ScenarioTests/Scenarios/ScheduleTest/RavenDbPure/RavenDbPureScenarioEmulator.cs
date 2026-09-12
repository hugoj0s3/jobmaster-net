using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.RavenDbPure;

public sealed class RavenDbPureScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<RavenDbPureClusters, RavenDbPurePhases>(global);
