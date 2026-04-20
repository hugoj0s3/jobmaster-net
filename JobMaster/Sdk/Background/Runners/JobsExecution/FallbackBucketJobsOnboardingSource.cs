using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

internal sealed class FallbackBucketJobsOnboardingSource : IJobsOnboardingSource
{
    private readonly List<JobRawModel> queue = new();
    private readonly int capacity;
    private readonly object objLock = new();

    public FallbackBucketJobsOnboardingSource(int capacity)
    {
        this.capacity = capacity;
    }

    public Task<bool> PushAsync(JobRawModel job)
    {
        lock (objLock)
        {
            if (queue.Count >= capacity)
                return Task.FromResult(false);

            queue.Add(job);
            return Task.FromResult(true);
        }
    }

    public Task<IList<JobRawModel>> TakeAsync(int count, DateTime scheduledAt)
    {
        lock (objLock)
        {
            var toReturn = queue
                .Where(j => j.NextPlanExecutionAt <= scheduledAt)
                .Take(count)
                .ToList();

            foreach (var j in toReturn)
                queue.Remove(j);

            return Task.FromResult<IList<JobRawModel>>(toReturn);
        }
    }
}
