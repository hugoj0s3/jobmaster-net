using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.FallbackBucketTest.PostgresPure;

public sealed class PostgresPureTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<PostgresPureScenarioEmulator, PostgresPureClusters, PostgresPurePhases>(global);
