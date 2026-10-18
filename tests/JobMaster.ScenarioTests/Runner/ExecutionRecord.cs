namespace JobMaster.ScenarioTests.Runner;

/// <summary><paramref name="ClusterId"/> is only recorded by apps that report it (TargetTestClusterIdApp); null otherwise.</summary>
public sealed record ExecutionRecord(Guid JobId, string DefinitionId, DateTime ExecutedAtUtc, string HostId, string? ClusterId = null);
