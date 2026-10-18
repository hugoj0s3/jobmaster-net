namespace TargetTestClusterIdApp;

/// <summary>
/// Must match the cluster ids in the ClusterIdTest scenario JSON (different process/project, so duplicated as
/// literals -- same pairing pattern as the JobDefinitionId strings between the other target apps and their
/// scenarios). Every cluster id referenced by an attribute/definition/profile here must be configured in every
/// container running this app, or startup validation fails -- which is why this lives in its own app instead
/// of the shared TargetTestScheduleApp/TargetTestRecurringApp.
/// </summary>
public static class ClusterIds
{
    public const string Target = "cluster-id-target";
}
