using System.Threading.Tasks;
using JobMaster.Sdk.Contracts.Models.Jobs;

namespace JobMaster.Sdk.Contracts.Background;

public interface IJobsExecutionEngine
{
    IOnBoardingControl<JobRawModel> OnBoardingControl { get; }
    ITaskQueueControl<JobRawModel> TaskQueueControl { get; }
    string BucketId { get; }
    
    Task<OnBoardingResult> TryOnBoardingJobAsync(JobRawModel payload, bool forceIfNoCapacity = false);

    Task FlushToMasterAsync();
    
    Task PulseAsync();
}
