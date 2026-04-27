using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Sdk.Abstractions.Background;

internal interface IJobsOnboardingSource
{
    Task<IList<JobRawModel>> PullAsync(int count, DateTime scheduledAt);
}
