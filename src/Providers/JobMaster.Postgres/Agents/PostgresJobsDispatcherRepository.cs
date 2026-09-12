using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Repositories;

namespace JobMaster.Postgres.Agents;

internal class PostgresJobsDispatcherRepository : AgentJobsDispatcherRepository<PostgresRawMessagesDispatcherRepository, PostgresRawMessagesDispatcherRepository>
{
    public PostgresJobsDispatcherRepository(
        JobMasterClusterConnectionConfig connectionConfig,
        IJobMasterLogger jobMasterLogger,
        PostgresRawMessagesDispatcherRepository savePendingRepository,
        PostgresRawMessagesDispatcherRepository processingRepository) : base(connectionConfig, savePendingRepository, processingRepository, jobMasterLogger)
    {
    }

    public override string AgentRepoTypeId => PostgresRepositoryConstants.RepositoryTypeId;
}