using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;

namespace JobMaster.UnitTests.Background;

[JobMasterDefinitionIdAttribute(FakeJobHandler.DefinitionId)]
internal sealed class FakeJobHandler : IJobHandler
{
    public const string DefinitionId = "fake-job";

    private int executionCount;
    public int ExecutionCount => executionCount;

    private readonly List<JobContext> executedJobs = new();
    public IReadOnlyList<JobContext> ExecutedJobs
    {
        get { lock (executedJobs) { return executedJobs.ToList(); } }
    }

    /// <summary>
    /// When set, the handler throws for any job where this delegate returns <see langword="true"/>.
    /// Use <c>_ => true</c> to fail all jobs, or filter by <see cref="JobContext"/> properties.
    /// </summary>
    public Func<JobContext, bool>? ShouldFail { get; set; }

    /// <summary>
    /// When set, <see cref="HandleAsync"/> awaits this source before returning, keeping the
    /// job occupying its TaskQueue slot. Call <c>BlockUntil.SetResult()</c> to release all
    /// blocked handlers at once.
    /// </summary>
    public TaskCompletionSource? BlockUntil { get; set; }

    public async Task HandleAsync(JobContext job)
    {
        if (BlockUntil != null)
            await BlockUntil.Task;

        if (ShouldFail?.Invoke(job) == true)
            throw new Exception("Simulated failure");

        Interlocked.Increment(ref executionCount);
        lock (executedJobs) { executedJobs.Add(job); }
    }
}
