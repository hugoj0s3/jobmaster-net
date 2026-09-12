namespace JobMaster.ScenarioTests.Runner;

public interface IExecutionTracker
{
    Task<IReadOnlyList<ExecutionRecord>> WaitForAsync(
        string testIdentifier, int expectedCount, TimeSpan timeout, CancellationToken ct = default);

    Task<IReadOnlyList<ExecutionRecord>> GetAllAsync(string testIdentifier, CancellationToken ct = default);

    /// <summary>
    /// Deletes any recorded executions for <paramref name="testIdentifier"/>. Redis is shared for
    /// the entire test run (<see cref="JobMaster.ScenarioTests.Fixtures.ScenarioGlobalEnvironment"/>),
    /// so a scenario that reuses a fixed identifier across runs (e.g. a static recurring schedule's
    /// identifier, baked into a shared app image) must call this before relying on an empty/fresh
    /// result -- otherwise it can observe executions left behind by an earlier scenario run.
    /// </summary>
    Task ClearAsync(string testIdentifier, CancellationToken ct = default);
}
