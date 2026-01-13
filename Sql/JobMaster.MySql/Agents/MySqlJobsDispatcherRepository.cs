using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Repositories;

namespace JobMaster.MySql.Agents;

public class MySqlJobsDispatcherRepository : AgentJobsDispatcherRepository<MySqlRawMessagesDispatcherRepository, MySqlRawMessagesDispatcherRepository>
{
    public MySqlJobsDispatcherRepository(
        JobMasterClusterConnectionConfig connectionConfig,
        IMasterClusterConfigurationService masterClusterConfigurationService,
        IJobMasterLogger logger,
        MySqlRawMessagesDispatcherRepository savePendingRepository,
        MySqlRawMessagesDispatcherRepository processingRepository) : base(connectionConfig, masterClusterConfigurationService, savePendingRepository, processingRepository, logger)
    {
    }

    public override string AgentRepoTypeId => MySqlRepositoryConstants.RepositoryTypeId;
}
