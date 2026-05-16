using JobMaster.MySql.Agents;
using JobMaster.MySql.Exceptions;
using JobMaster.MySql.Master;
using JobMaster.SqlBase.Connections;
using JobMaster.Sdk;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Ioc.Setup;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.MySql;

[JobMasterIocRegistration]
internal static class MySqlIocRegistration
{
    public static string RepositoryType => MySqlRepositoryConstants.RepositoryTypeId;

    public static void RegisterForMaster(ClusterIocRegistration registration, string clusterId)
    {
        registration.ClusterServices.AddKeyedSingleton<IDbConnectionManager, MySqlDbConnectionManager>(RepositoryType);
        registration.ClusterServices.AddSingleton<IDbConnectionManager>(provider => provider.GetRequiredKeyedService<IDbConnectionManager>(RepositoryType));
        
        registration.AddJobMasterComponent<IMasterGenericRecordRepository, MySqlMasterGenericRecordRepository>();
        registration.AddJobMasterComponent<IMasterDistributedLockerRepository, MySqlMasterDistributedLockerRepository>();
        registration.AddJobMasterComponent<IMasterJobsRepository, MySqlMasterJobsRepository>();
        registration.AddJobMasterComponent<IMasterRecurringSchedulesRepository, MySqlMasterRecurringSchedulesRepository>();
        registration.AddJobMasterComponent<IMasterLogsRepository, MySqlMasterLogsRepository>();
        registration.ClusterServices.AddSingleton<IKnownExceptionIdentifierStrategy, MySqlKnownExceptionIdentifierStrategy>();
    }

    public static void RegisterForAgent(ClusterIocRegistration registration, string clusterId)
    {
        registration.ClusterServices.AddKeyedSingleton<IDbConnectionManager, MySqlDbConnectionManager>(RepositoryType);
        registration.AddFootprintResolver<MySqlAgentFootprintResolver>(RepositoryType);
        registration.AddRepositoryDispatcher<MySqlJobsDispatcherRepository, MySqlRawMessagesDispatcherRepository, MySqlRawMessagesDispatcherRepository>(RepositoryType);
    }
}
