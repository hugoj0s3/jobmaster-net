using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Sdk.Abstractions.Background;

internal interface IJobsExecutionEngine
{
    IOnBoardingControl<JobRawModel> OnBoardingControl { get; }
    ITaskQueueControl<JobRawModel> TaskQueueControl { get; }
    string BucketId { get; }

    bool HasOnBoardingAvailability();
    int CountOnBoardingAvailability();

    Task<OnBoardingResult> TryOnBoardingJobAsync(JobRawModel payload, bool forceIfNoCapacity = false);

    Task FlushToMasterAsync();

    Task PulseAsync();
}