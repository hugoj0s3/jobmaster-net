using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.FallbackBucketTest.MySqlPure;

public sealed class MySqlPureScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<MySqlPureClusters, MySqlPurePhases>(global);
