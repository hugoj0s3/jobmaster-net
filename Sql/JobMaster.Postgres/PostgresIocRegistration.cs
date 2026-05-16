using JobMaster.Postgres.Agents;
using JobMaster.Postgres.Exceptions;
using JobMaster.Postgres.Master;
using JobMaster.SqlBase.Connections;
using JobMaster.Sdk;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Repositories.Master;
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
        registration.AddJobMasterComponent<IMasterLogsRepository, PostgresMasterLogsRepository>();
        registration.ClusterServices.AddSingleton<IKnownExceptionIdentifierStrategy, PostgresKnownExceptionIdentifierStrategy>();
    }
    
    public static void RegisterForAgent(ClusterIocRegistration registration, string clusterId)
    {
        registration.ClusterServices.AddKeyedSingleton<IDbConnectionManager, PostgresDbConnectionManager>(RepositoryType);
        registration.AddFootprintResolver<PostgresAgentFootprintResolver>(RepositoryType);
        registration.AddRepositoryDispatcher<PostgresJobsDispatcherRepository, PostgresRawMessagesDispatcherRepository, PostgresRawMessagesDispatcherRepository>(PostgresRepositoryConstants.RepositoryTypeId);
    }
}
