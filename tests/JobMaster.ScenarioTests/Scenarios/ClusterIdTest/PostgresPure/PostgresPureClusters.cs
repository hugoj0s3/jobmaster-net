namespace JobMaster.ScenarioTests.Scenarios.ClusterIdTest.PostgresPure;

// Member names -> kebab-cased ClusterId (see ClusterConfigBuilder.ClusterId). "cluster-id-target" must match
// TargetTestClusterIdApp's ClusterIds.Target.
public enum PostgresPureClusters
{
    ClusterIdDefault = 1, // "cluster-id-default"
    ClusterIdTarget = 2   // "cluster-id-target"
}
