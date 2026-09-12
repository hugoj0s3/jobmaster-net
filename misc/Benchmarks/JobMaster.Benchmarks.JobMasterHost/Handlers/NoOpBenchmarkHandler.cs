using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Benchmarks.Common.Recording;

namespace JobMaster.Benchmarks.JobMasterHost.Handlers;

/// <summary>
/// Simulates a real job body (see <see cref="SimulatedJobWork"/>) then a single Redis write on
/// completion. Isolates the benchmark to JobMaster's own scheduling/dispatch overhead measured
/// against a realistic job duration, rather than an instant no-op -- every other framework's
/// benchmark host does the identical delay-then-record contract, for a fair comparison.
/// </summary>
[JobMasterDefinitionId("Benchmark.NoOp")]
public sealed class NoOpBenchmarkHandler(ICompletionRecorder recorder) : IJobMasterHandler
{
    public async Task HandleAsync(JobContext job)
    {
        await SimulatedJobWork.DelayAsync();
        await recorder.RecordCompletionAsync(job.Id);
    }
}
