using System.Text.Json;
using StackExchange.Redis;

namespace TargetTestClusterIdApp.Redis;

/// <summary>
/// Same Redis record shape as the other target apps' recorders, plus the <c>ClusterId</c> the job actually
/// ran on -- the whole point of this app is to prove which cluster each job landed on.
/// </summary>
public sealed class ExecutionRecorder(IConnectionMultiplexer mux)
{
    public async Task RecordAsync(string testIdentifier, Guid jobId, string definitionId, string clusterId)
    {
        var db = mux.GetDatabase();
        var record = JsonSerializer.Serialize(new
        {
            JobId = jobId,
            DefinitionId = definitionId,
            ExecutedAtUtc = DateTime.UtcNow,
            HostId = Environment.MachineName,
            ClusterId = clusterId
        });

        await db.ListRightPushAsync($"scenario:{testIdentifier}:executions", record);
    }
}
