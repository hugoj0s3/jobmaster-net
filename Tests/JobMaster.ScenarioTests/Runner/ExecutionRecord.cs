namespace JobMaster.ScenarioTests.Runner;

public sealed record ExecutionRecord(Guid JobId, string DefinitionId, DateTime ExecutedAtUtc, string HostId);
