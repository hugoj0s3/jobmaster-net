using JobMaster.Sdk;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Ioc.Setup;
using JobMaster.Sql.Connections;
using JobMaster.SqlServer.Agents;
using JobMaster.SqlServer.Master;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.SqlServer;

[JobMasterIocRegistration]
internal static class SqlServerIocRegistration
{
    public static string RepositoryType => SqlServerRepositoryConstants.RepositoryTypeId;

    public static void RegisterForMaster(ClusterIocRegistration registration, string clusterId)
    {
        registration.ClusterServices.AddKeyedSingleton<IDbConnectionManager, SqlServerDbConnectionManager>(RepositoryType);
        registration.ClusterServices.AddSingleton<IDbConnectionManager>(provider => provider.GetRequiredKeyedService<IDbConnectionManager>(RepositoryType));

        registration.AddJobMasterComponent<IMasterGenericRecordRepository, SqlServerMasterGenericRecordRepository>();
        registration.AddJobMasterComponent<IMasterDistributedLockerRepository, SqlServerMasterDistributedLockerRepository>();
        registration.AddJobMasterComponent<IMasterJobsRepository, SqlServerMasterJobsRepository>();
        registration.AddJobMasterComponent<IMasterRecurringSchedulesRepository, SqlServerMasterRecurringSchedulesRepository>();
    }

    public static void RegisterForAgent(ClusterIocRegistration registration, string clusterId)
    {
        registration.ClusterServices.AddKeyedSingleton<IDbConnectionManager, SqlServerDbConnectionManager>(RepositoryType);
        registration.AddRepositoryDispatcher<SqlServerJobsDispatcherRepository, SqlServerRawMessagesDispatcherRepository, SqlServerRawMessagesDispatcherRepository>(RepositoryType);
    }
}
