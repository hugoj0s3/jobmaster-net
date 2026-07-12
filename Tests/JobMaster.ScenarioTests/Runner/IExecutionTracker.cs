namespace JobMaster.ScenarioTests.Runner;

public interface IExecutionTracker
{
    Task<IReadOnlyList<ExecutionRecord>> WaitForAsync(
        string testIdentifier, int expectedCount, TimeSpan timeout, CancellationToken ct = default);

    Task<IReadOnlyList<ExecutionRecord>> GetAllAsync(string testIdentifier, CancellationToken ct = default);
}
