using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Benchmarks.Common.Recording;

namespace JobMaster.Benchmarks.JobMasterHost.Handlers;

/// <summary>
/// A true no-op handler: no artificial delay, a single Redis write on completion. Isolates the
/// benchmark to JobMaster's own scheduling/dispatch overhead rather than handler execution time --
/// every other framework's benchmark host does the identical single-write completion recording, for
/// a fair comparison.
/// </summary>
[JobMasterDefinitionId("Benchmark.NoOp")]
public sealed class NoOpBenchmarkHandler(ICompletionRecorder recorder) : IJobMasterHandler
{
    public Task HandleAsync(JobContext job) => recorder.RecordCompletionAsync(job.Id);
}
