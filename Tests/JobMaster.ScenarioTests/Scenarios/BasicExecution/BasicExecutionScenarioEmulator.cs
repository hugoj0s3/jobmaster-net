using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.BasicExecution;

public sealed class BasicExecutionScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<BasicExecutionClusters, BasicExecutionPhases>(global);
