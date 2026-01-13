using JobMaster.Postgres.Agents;
using JobMaster.Postgres.Master;
using JobMaster.Sql.Connections;
using JobMaster.Sdk;
using JobMaster.Sdk.Contracts.Repositories.Master;
using JobMaster.Sdk.Ioc.Setup;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Postgres;

[JobMasterIocRegistration]
internal static class PostgresIocRegistration
{
    public static string RepositoryType => PostgresRepositoryConstants.RepositoryTypeId;
    
    public static void RegisterForMaster(ClusterIocRegistration registration, string clusterId)
    {
        registration.ClusterServices.AddKeyedSingleton<IDbConnectionManager, PostgresDbConnectionManager>(RepositoryType);
        registration.ClusterServices.AddSingleton<IDbConnectionManager>(provider => provider.GetRequiredKeyedService<IDbConnectionManager>(RepositoryType));
        registration.AddJobMasterComponent<IMasterGenericRecordRepository, PostgresMasterGenericRecordRepository>();
        registration.AddJobMasterComponent<IMasterDistributedLockerRepository, PostgresMasterDistributedLockerRepository>();
        registration.AddJobMasterComponent<IMasterJobsRepository, PostgresMasterJobsRepository>();
        registration.AddJobMasterComponent<IMasterRecurringSchedulesRepository, PostgresMasterRecurringSchedulesRepository>();
    }
    
    public static void RegisterForAgent(ClusterIocRegistration registration, string clusterId)
    {
        registration.ClusterServices.AddKeyedSingleton<IDbConnectionManager, PostgresDbConnectionManager>(RepositoryType);
        registration.AddRepositoryDispatcher<PostgresJobsDispatcherRepository, PostgresRawMessagesDispatcherRepository, PostgresRawMessagesDispatcherRepository>(PostgresRepositoryConstants.RepositoryTypeId);
    }
}
