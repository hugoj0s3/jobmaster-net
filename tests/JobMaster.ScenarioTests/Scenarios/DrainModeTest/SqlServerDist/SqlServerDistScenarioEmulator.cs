using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.SqlServerDist;

public sealed class SqlServerDistScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<SqlServerDistClusters, SqlServerDistPhases>(global);
