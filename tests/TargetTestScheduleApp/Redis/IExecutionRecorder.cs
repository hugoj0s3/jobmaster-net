namespace TargetTestScheduleApp.Redis;

public interface IExecutionRecorder
{
    Task RecordAsync(string testIdentifier, Guid jobId, string definitionId, CancellationToken ct = default);
}
