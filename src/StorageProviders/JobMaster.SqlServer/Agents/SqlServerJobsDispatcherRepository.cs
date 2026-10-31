using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Repositories;

namespace JobMaster.SqlServer.Agents;

internal class SqlServerJobsDispatcherRepository : AgentJobsDispatcherRepository<SqlServerRawMessagesDispatcherRepository, SqlServerRawMessagesDispatcherRepository>
{
    public SqlServerJobsDispatcherRepository(
        JobMasterClusterConnectionConfig connectionConfig,
        IJobMasterLogger logger,
        SqlServerRawMessagesDispatcherRepository savePendingRepository,
        SqlServerRawMessagesDispatcherRepository processingRepository) : base(connectionConfig, savePendingRepository, processingRepository, logger)
    {
    }

    public override string AgentRepoTypeId => SqlServerRepositoryConstants.RepositoryTypeId;
}
