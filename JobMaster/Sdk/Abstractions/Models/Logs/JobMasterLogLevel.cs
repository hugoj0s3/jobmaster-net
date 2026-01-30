namespace JobMaster.Sdk.Abstractions.Models.Logs;

internal enum JobMasterLogLevel
{
    Debug = 0,
    Info = 1,
    Warning,
    Error,
    Critical
}

internal enum JobMasterLogSubjectType
{
    Job = 1,
    JobExecution,
    AgentWorker,
    Bucket,
    Cluster,
    RecurringSchedule,
    Api
}