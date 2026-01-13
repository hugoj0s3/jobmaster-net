namespace JobMaster.Sdk.Contracts.Models.Logs;

public enum JobMasterLogLevel
{
    Debug = 0,
    Info = 1,
    Warning,
    Error,
    Critical
}

public enum JobMasterLogSubjectType
{
    Job = 1,
    JobExecution,
    AgentWorker,
    Bucket,
    Cluster,
    RecurringSchedule
}