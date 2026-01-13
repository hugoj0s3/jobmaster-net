namespace JobMaster.Contracts.Models;

public enum JobMasterJobStatus
{
    SavePending = 1,
    HeldOnMaster = 2,
    AssignedToBucket = 3,
    Processing = 4,
    Succeeded = 5,
    Queued = 6,
    Failed = 7,
    Cancelled = 8,
}

public static class JobMasterJobStatusUtil
{
    public static bool IsFinalStatus(this JobMasterJobStatus jobStatus) => GetFinalStatuses().Contains(jobStatus);
    
    public static bool IsBucketStatus(this JobMasterJobStatus jobStatus) => GetBucketStatuses().Contains(jobStatus);
    
    public static IList<JobMasterJobStatus> GetFinalStatuses() => 
        new List<JobMasterJobStatus> { JobMasterJobStatus.Succeeded, JobMasterJobStatus.Failed, JobMasterJobStatus.Cancelled };
    
    public static IList<JobMasterJobStatus> GetBucketStatuses() => 
        new List<JobMasterJobStatus> { JobMasterJobStatus.AssignedToBucket, JobMasterJobStatus.Queued };
}