using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

internal sealed class FallbackBucketJobsOnboardingSource : IJobsOnboardingSource
{
    private readonly List<JobRawModel> queue = new();
    private readonly object objLock = new();

    public Task PushAsync(JobRawModel job)
    {
        lock (objLock)
        {
            queue.Add(job);
            return Task.CompletedTask;
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
