using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest.SqlServerPure;

public sealed class SqlServerPureTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<SqlServerPureScenarioEmulator, SqlServerPureClusters, SqlServerPurePhases>(global);
