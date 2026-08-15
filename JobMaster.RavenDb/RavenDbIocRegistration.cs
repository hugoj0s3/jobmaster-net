using JobMaster.RavenDb.Agents;
using JobMaster.RavenDb.Connections;
using JobMaster.RavenDb.Master;
using JobMaster.Sdk;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Ioc.Setup;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.RavenDb;

[JobMasterIocRegistration]
internal static class RavenDbIocRegistration
{
    public static string RepositoryType => RavenDbRepositoryConstants.RepositoryTypeId;

    public static void RegisterForMaster(ClusterIocRegistration registration, string clusterId)
    {
        registration.ClusterServices.AddKeyedSingleton<IRavenDbDocumentStoreManager, RavenDbDocumentStoreManager>(RepositoryType);
        registration.ClusterServices.AddSingleton<IRavenDbDocumentStoreManager>(sp => sp.GetRequiredKeyedService<IRavenDbDocumentStoreManager>(RepositoryType));
        registration.AddJobMasterComponent<IMasterDistributedLockerRepository, RavenDbMasterDistributedLockerRepository>();
        registration.AddJobMasterComponent<IMasterGenericRecordRepository, RavenDbMasterGenericRecordRepository>();
        registration.AddJobMasterComponent<IMasterLogsRepository, RavenDbMasterLogsRepository>();
        // Other 2 Master interfaces land in later increments, one at a time -- see the RavenDB plan's
        // roadmap for the order.
    }

    public static void RegisterForAgent(ClusterIocRegistration registration, string clusterId)
    {
        registration.ClusterServices.AddKeyedSingleton<IRavenDbDocumentStoreManager, RavenDbDocumentStoreManager>(RepositoryType);
        registration.ClusterServices.AddSingleton<IRavenDbDocumentStoreManager>(sp => sp.GetRequiredKeyedService<IRavenDbDocumentStoreManager>(RepositoryType));
        registration.AddFingerprintResolver<RavenDbAgentFingerprintResolver>(RepositoryType);
        registration.AddRepositoryDispatcher<RavenDbJobsDispatcherRepository, RavenDbRawMessagesDispatcherRepository, RavenDbRawMessagesDispatcherRepository>(RepositoryType);
    }
}
