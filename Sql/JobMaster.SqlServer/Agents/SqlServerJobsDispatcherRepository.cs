using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Repositories;

namespace JobMaster.SqlServer.Agents;

internal class SqlServerJobsDispatcherRepository : AgentJobsDispatcherRepository<SqlServerRawMessagesDispatcherRepository, SqlServerRawMessagesDispatcherRepository>
{
    public SqlServerJobsDispatcherRepository(
        JobMasterClusterConnectionConfig connectionConfig,
        IMasterClusterConfigurationService masterClusterConfigurationService,
        IJobMasterLogger logger,
        SqlServerRawMessagesDispatcherRepository savePendingRepository,
        SqlServerRawMessagesDispatcherRepository processingRepository) : base(connectionConfig, masterClusterConfigurationService, savePendingRepository, processingRepository, logger)
    {
    }

    public override string AgentRepoTypeId => SqlServerRepositoryConstants.RepositoryTypeId;
}
