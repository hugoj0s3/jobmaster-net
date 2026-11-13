using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest.HybridNatsRavenDb;

public sealed class HybridNatsRavenDbTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<HybridNatsRavenDbScenarioEmulator, HybridNatsRavenDbClusters, HybridNatsRavenDbPhases>(global);
