namespace JobMaster.Api.ApiModels;

public enum ApiJobMasterLogSubjectType
{
    Job = 1,
    JobExecution = 2,
    AgentWorker = 3,
    Bucket = 4,
    Cluster = 5,
    RecurringSchedule = 6,
    Api = 7,
}