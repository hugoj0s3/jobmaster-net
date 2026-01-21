using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Sdk.Abstractions.Background;

public interface IJobsExecutionEngine
{
    IOnBoardingControl<JobRawModel> OnBoardingControl { get; }
    ITaskQueueControl<JobRawModel> TaskQueueControl { get; }
    string BucketId { get; }
    
    Task<OnBoardingResult> TryOnBoardingJobAsync(JobRawModel payload, bool forceIfNoCapacity = false);

    Task FlushToMasterAsync();
    
    Task PulseAsync();
}
