using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.ArchivedModeTest.SqlServerPure;

public sealed class SqlServerPureTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<SqlServerPureScenarioEmulator, SqlServerPureClusters, SqlServerPurePhases>(global);
