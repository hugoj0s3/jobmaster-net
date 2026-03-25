namespace JobMaster.Abstractions.Models;

public enum JobMasterJobStatus
{
    PendingSave = 1,
    OnMaster = 2,
    InBucket = 3,
    Processing = 4,
    Succeeded = 5,
    Queued = 6,
    Failed = 7,
    Cancelled = 8,
    Aborted = 9,
}

public static class JobMasterJobStatusUtil
{
    public static bool IsFinalStatus(this JobMasterJobStatus jobStatus) => GetFinalStatuses().Contains(jobStatus);
    
    public static bool IsBucketStatus(this JobMasterJobStatus jobStatus) => GetBucketStatuses().Contains(jobStatus);
    
    public static IList<JobMasterJobStatus> GetFinalStatuses() => 
        new List<JobMasterJobStatus>
        {
            JobMasterJobStatus.Succeeded, 
            JobMasterJobStatus.Failed, 
            JobMasterJobStatus.Cancelled, 
            JobMasterJobStatus.Aborted
        };
    
    public static IList<JobMasterJobStatus> GetBucketStatuses() => 
        new List<JobMasterJobStatus> { JobMasterJobStatus.InBucket, JobMasterJobStatus.Queued };
}