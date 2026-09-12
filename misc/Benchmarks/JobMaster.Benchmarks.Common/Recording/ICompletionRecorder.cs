namespace JobMaster.Benchmarks.Common.Recording;

/// <summary>Written by each framework host's no-op job handler on execution -- shared across all
/// four frameworks' hosts so completion recording is identical (list-append, not overwrite, so a
/// job executed more than once produces more than one entry -- see <see cref="LatencyJoiner"/>'s
/// duplicated-job detection).</summary>
public interface ICompletionRecorder
{
    Task RecordCompletionAsync(Guid jobId, CancellationToken ct = default);
}
