using System.Text.Json;
using StackExchange.Redis;

namespace TargetTestScheduleApp.Redis;

public sealed class RedisExecutionRecorder(IConnectionMultiplexer mux) : IExecutionRecorder
{
    public async Task RecordAsync(string testIdentifier, Guid jobId, string definitionId, CancellationToken ct = default)
    {
        var db = mux.GetDatabase();
        var record = JsonSerializer.Serialize(new
        {
            JobId = jobId,
            DefinitionId = definitionId,
            ExecutedAtUtc = DateTime.UtcNow,
            HostId = Environment.MachineName
        });

        await db.ListRightPushAsync($"scenario:{testIdentifier}:executions", record);
    }
}
