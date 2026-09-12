using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.SqlServerDist;

public sealed class SqlServerDistTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<SqlServerDistScenarioEmulator, SqlServerDistClusters, SqlServerDistPhases>(global);
