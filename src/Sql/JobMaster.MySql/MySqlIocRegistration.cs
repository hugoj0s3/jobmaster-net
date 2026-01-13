using JobMaster.MySql.Agents;
using JobMaster.MySql.Master;
using JobMaster.Sql.Connections;
using JobMaster.Sdk;
using JobMaster.Sdk.Contracts.Repositories.Master;
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
    }

    public static void RegisterForAgent(ClusterIocRegistration registration, string clusterId)
    {
        registration.ClusterServices.AddKeyedSingleton<IDbConnectionManager, MySqlDbConnectionManager>(RepositoryType);
        registration.AddRepositoryDispatcher<MySqlJobsDispatcherRepository, MySqlRawMessagesDispatcherRepository, MySqlRawMessagesDispatcherRepository>(RepositoryType);
    }
}
