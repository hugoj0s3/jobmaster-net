using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Repositories;

namespace JobMaster.Postgres.Agents;

public class PostgresJobsDispatcherRepository : AgentJobsDispatcherRepository<PostgresRawMessagesDispatcherRepository, PostgresRawMessagesDispatcherRepository>
{
    public PostgresJobsDispatcherRepository(
        JobMasterClusterConnectionConfig connectionConfig,
        IMasterClusterConfigurationService masterClusterConfigurationService,
        IJobMasterLogger jobMasterLogger,
        PostgresRawMessagesDispatcherRepository savePendingRepository, 
        PostgresRawMessagesDispatcherRepository processingRepository) : base(connectionConfig, masterClusterConfigurationService, savePendingRepository, processingRepository, jobMasterLogger)
    {
    }

    public override string AgentRepoTypeId => PostgresRepositoryConstants.RepositoryTypeId;
}