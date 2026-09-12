using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Repositories;

namespace JobMaster.MySql.Agents;

internal class MySqlJobsDispatcherRepository : AgentJobsDispatcherRepository<MySqlRawMessagesDispatcherRepository, MySqlRawMessagesDispatcherRepository>
{
    public MySqlJobsDispatcherRepository(
        JobMasterClusterConnectionConfig connectionConfig,
        IJobMasterLogger logger,
        MySqlRawMessagesDispatcherRepository savePendingRepository,
        MySqlRawMessagesDispatcherRepository processingRepository) : base(connectionConfig, savePendingRepository, processingRepository, logger)
    {
    }

    public override string AgentRepoTypeId => MySqlRepositoryConstants.RepositoryTypeId;
}
