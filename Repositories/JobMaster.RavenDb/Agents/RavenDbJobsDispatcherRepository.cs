using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Repositories;

namespace JobMaster.RavenDb.Agents;

internal sealed class RavenDbJobsDispatcherRepository : AgentJobsDispatcherRepository<RavenDbRawMessagesDispatcherRepository, RavenDbRawMessagesDispatcherRepository>
{
    public RavenDbJobsDispatcherRepository(
        JobMasterClusterConnectionConfig connectionConfig,
        IJobMasterLogger jobMasterLogger,
        RavenDbRawMessagesDispatcherRepository savePendingRepository,
        RavenDbRawMessagesDispatcherRepository processingRepository) : base(connectionConfig, savePendingRepository, processingRepository, jobMasterLogger)
    {
    }

    public override string AgentRepoTypeId => RavenDbRepositoryConstants.RepositoryTypeId;
}
