using System.Text.Json;
using StackExchange.Redis;

namespace JobMaster.ScenarioTests.Runner;

public sealed class ExecutionTracker(IConnectionMultiplexer mux) : IExecutionTracker
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(250);

    public async Task<IReadOnlyList<ExecutionRecord>> WaitForAsync(
        string testIdentifier, int expectedCount, TimeSpan timeout, CancellationToken ct = default)
    {
        var deadline = DateTime.UtcNow + timeout;
        IReadOnlyList<ExecutionRecord> last = Array.Empty<ExecutionRecord>();

        while (DateTime.UtcNow < deadline)
        {
            last = await GetAllAsync(testIdentifier, ct);
            if (last.Count >= expectedCount)
            {
                return last;
            }

            await Task.Delay(PollInterval, ct);
        }

        throw new TimeoutException(
            $"Timed out waiting for {expectedCount} execution(s) for TestIdentifier '{testIdentifier}'. " +
            $"Observed {last.Count}: [{string.Join(", ", last.Select(r => r.JobId))}].");
    }

    public async Task<IReadOnlyList<ExecutionRecord>> GetAllAsync(string testIdentifier, CancellationToken ct = default)
    {
        var db = mux.GetDatabase();
        var values = await db.ListRangeAsync($"scenario:{testIdentifier}:executions");

        return values
            .Select(v => JsonSerializer.Deserialize<ExecutionRecord>(v.ToString(), ScenarioJsonOptions.Default)!)
            .ToList();
    }

    public async Task ClearAsync(string testIdentifier, CancellationToken ct = default)
    {
        var db = mux.GetDatabase();
        await db.KeyDeleteAsync($"scenario:{testIdentifier}:executions");
    }
}
