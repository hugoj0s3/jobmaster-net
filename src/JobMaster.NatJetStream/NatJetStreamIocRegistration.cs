using JobMaster.NatJetStream.Agents;
using JobMaster.NatJetStream.Background;
using JobMaster.Sdk;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Ioc.Setup;

namespace JobMaster.NatJetStream;

[JobMasterIocRegistration]
internal static class NatJetStreamIocRegistration
{
    public static string RepositoryType => NatJetStreamConstants.RepositoryTypeId;
    
    public static void RegisterForAgent(ClusterIocRegistration registration, string clusterId)
    {
        // Wire the generic dispatcher to use NatJetStream raw repositories
        registration.AddRepositoryDispatcher<NatJetStreamJobsDispatcherRepository, NatJetStreamRawMessagesDispatcherRepository, NatJetStreamRawMessagesDispatcherRepository>(RepositoryType);
       
        // Bucket-aware runners for JetStream transport
        registration.AddBucketAwareRunner<IJobsExecutionRunner, NatJetStreamJobsExecutionRunner>(RepositoryType);
        registration.AddBucketAwareRunner<ISavePendingJobsRunner, NatJetStreamSavePendingJobsRunner>(RepositoryType);
        registration.AddBucketAwareRunner<IDrainSavePendingJobsRunner, NatJetStreamDrainSavePendingJobsRunner>(RepositoryType);
        registration.AddBucketAwareRunner<IDrainProcessingJobsRunner, NatJetStreamDrainProcessingRunner>(RepositoryType);
        registration.AddBucketAwareRunner<IDrainSavePendingRecurringScheduleRunner, NatJetStreamDrainSavePendingRecurringScheduleRunner>(RepositoryType);
        registration.AddBucketAwareRunner<ISaveRecurringSchedulerRunner, NetJetStreamSaveRecurringScheduleRunner>(RepositoryType);
    }
}
