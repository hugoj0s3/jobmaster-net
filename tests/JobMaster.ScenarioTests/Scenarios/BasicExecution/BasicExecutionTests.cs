using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.BasicExecution;

public sealed class BasicExecutionTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<BasicExecutionScenarioEmulator, BasicExecutionClusters, BasicExecutionPhases>(global);
