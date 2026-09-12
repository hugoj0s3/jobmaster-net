using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest.RavenDbPure;

public sealed class RavenDbPureScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<RavenDbPureClusters, RavenDbPurePhases>(global);
