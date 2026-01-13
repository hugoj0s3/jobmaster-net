using JobMaster.NatJetStreams.Agents;
using JobMaster.NatJetStreams.Background;
using JobMaster.Sdk;
using JobMaster.Sdk.Contracts.Background.Runners;
using JobMaster.Sdk.Ioc.Setup;

namespace JobMaster.NatJetStreams;

[JobMasterIocRegistration]
internal static class NatJetStreamIocRegistration
{
    public static string RepositoryType => NatJetStreamConstants.RepositoryTypeId;
    
    public static void RegisterForAgent(ClusterIocRegistration registration, string clusterId)
    {
        // Wire the generic dispatcher to use NatJetStream raw repositories
        registration.AddRepositoryDispatcher<NatJetStreamJobsDispatcherRepository, NatJetStreamsRawMessagesDispatcherRepository, NatJetStreamsRawMessagesDispatcherRepository>(RepositoryType);
       
        // Bucket-aware runners for JetStream transport
        registration.AddBucketAwareRunner<IJobsExecutionRunner, NatJetStreamJobsExecutionRunner>(RepositoryType);
        registration.AddBucketAwareRunner<ISavePendingJobsRunner, NatJetStreamSavePendingJobsRunner>(RepositoryType);
        registration.AddBucketAwareRunner<IDrainSavePendingJobsRunner, NatJetStreamDrainSavePendingJobsRunner>(RepositoryType);
        registration.AddBucketAwareRunner<IDrainProcessingJobsRunner, NatJetStreamDrainProcessingRunner>(RepositoryType);
        registration.AddBucketAwareRunner<IDrainSavePendingRecurringScheduleRunner, NatJetStreamDrainSavePendingRecurringScheduleRunner>(RepositoryType);
        registration.AddBucketAwareRunner<ISaveRecurringSchedulerRunner, NetJetStreamSaveRecurringScheduleRunner>(RepositoryType);
    }
}
