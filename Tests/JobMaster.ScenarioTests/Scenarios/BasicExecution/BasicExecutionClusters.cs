namespace JobMaster.ScenarioTests.Scenarios.BasicExecution;

/// <summary>
/// Type name (stripped of the Clusters suffix, kebab-cased) is what BaseScenarioEmulator uses to
/// locate this scenario's folder under Scenarios/ — "BasicExecutionClusters" -> "basic-execution".
/// Member names (kebab-cased the same way) are the actual ClusterId values used in scenario JSON.
/// </summary>
public enum BasicExecutionClusters
{
    BasicExecutionCluster = 1
}
