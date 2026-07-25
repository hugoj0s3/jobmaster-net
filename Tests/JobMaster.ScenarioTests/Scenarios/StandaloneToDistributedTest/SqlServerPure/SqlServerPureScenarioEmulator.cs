using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest.SqlServerPure;

public sealed class SqlServerPureScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<SqlServerPureClusters, SqlServerPurePhases>(global);
