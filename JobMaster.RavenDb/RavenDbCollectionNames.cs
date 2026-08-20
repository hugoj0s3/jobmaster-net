namespace JobMaster.RavenDb;

// Every RavenDB collection name this provider creates, in one place -- avoids repositories reaching into
// each other's internals (RavenDbJobMasterRuntimeSetup/RavenDbRepositoryFixture previously read
// RavenDbMasterJobsRepository.Collection and RavenDbRawMessagesDispatcherRepository.Collection directly)
// just to build a prefix-qualified collection name for index deployment.
internal static class RavenDbCollectionNames
{
    public const string Job = "Job";
    public const string JobExecution = "JobExecution";
    public const string RecurringSchedule = "RecurringSchedule";
    public const string Message = "Message";
    public const string Logs = "Logs";
    public const string Fingerprint = "fingerprint";
    public const string Lock = "lock";
}
