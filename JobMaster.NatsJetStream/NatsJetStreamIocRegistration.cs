using JobMaster.NatsJetStream.Agents;
using JobMaster.NatsJetStream.Background;
using JobMaster.Sdk;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Ioc.Setup;

namespace JobMaster.NatsJetStream;

[JobMasterIocRegistration]
internal static class NatsJetStreamIocRegistration
{
    public static string RepositoryType => NatsJetStreamConstants.RepositoryTypeId;
    
    public static void RegisterForAgent(ClusterIocRegistration registration, string clusterId)
    {
        // Wire the generic dispatcher to use NatsJetStream raw repositories
        registration.AddRepositoryDispatcher<NatsJetStreamJobsDispatcherRepository, NatsJetStreamRawMessagesDispatcherRepository, NatsJetStreamRawMessagesDispatcherRepository>(RepositoryType);
       
        // Bucket-aware runners for JetStream transport
        registration.AddBucketAwareRunner<IJobsExecutionRunner, NatsJetStreamJobsExecutionRunner>(RepositoryType);
        registration.AddBucketAwareRunner<ISavePendingJobsRunner, NatsJetStreamSavePendingJobsRunner>(RepositoryType);
        registration.AddBucketAwareRunner<IDrainSavePendingJobsRunner, NatsJetStreamDrainSavePendingJobsRunner>(RepositoryType);
        registration.AddBucketAwareRunner<IDrainProcessingJobsRunner, NatsJetStreamDrainProcessingRunner>(RepositoryType);
        registration.AddBucketAwareRunner<IDrainSavePendingRecurringScheduleRunner, NatsJetStreamDrainSavePendingRecurringScheduleRunner>(RepositoryType);
        registration.AddBucketAwareRunner<ISaveRecurringSchedulerRunner, NetsJetStreamSaveRecurringScheduleRunner>(RepositoryType);
    }
}
