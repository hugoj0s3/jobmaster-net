using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Sdk.Abstractions.Background;

internal interface IJobsOnboardingSource
{
    Task<bool> PushAsync(JobRawModel job);
    Task<IList<JobRawModel>> TakeAsync(int count, DateTime scheduledAt);
}