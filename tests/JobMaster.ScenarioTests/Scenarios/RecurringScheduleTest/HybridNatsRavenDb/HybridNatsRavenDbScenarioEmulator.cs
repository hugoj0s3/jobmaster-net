using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest.HybridNatsRavenDb;

public sealed class HybridNatsRavenDbScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<HybridNatsRavenDbClusters, HybridNatsRavenDbPhases>(global);
