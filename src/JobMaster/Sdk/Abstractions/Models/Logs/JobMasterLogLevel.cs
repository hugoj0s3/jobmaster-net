using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Models.Logs;

[EditorBrowsable(EditorBrowsableState.Never)]
public enum JobMasterLogLevel
{
    Debug = 0,
    Info = 1,
    Warning,
    Error,
    Critical
}

[EditorBrowsable(EditorBrowsableState.Never)]
public enum JobMasterLogSubjectType
{
    Job = 1,
    JobExecution,
    AgentWorker,
    Bucket,
    Cluster,
    RecurringSchedule
}