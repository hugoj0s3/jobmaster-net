namespace JobMaster.Api.ApiModels;

/// <summary>Categorizes a log entry by the system component that produced it.</summary>
public enum ApiJobMasterLogCategory
{
    /// <summary>Log entry relates to a job lifecycle event.</summary>
    Job = 1,
    /// <summary>Log entry relates to a job execution event.</summary>
    JobExecution = 2,
    /// <summary>Log entry relates to an agent worker event.</summary>
    AgentWorker = 3,
    /// <summary>Log entry relates to a bucket lifecycle event.</summary>
    Bucket = 4,
    /// <summary>Log entry relates to a cluster-level event.</summary>
    Cluster = 5,
    /// <summary>Log entry relates to a recurring schedule event.</summary>
    RecurringSchedule = 6,
    /// <summary>Log entry originates from the API layer.</summary>
    Api = 7,
}
