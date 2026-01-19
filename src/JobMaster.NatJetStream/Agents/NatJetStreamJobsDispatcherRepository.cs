using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Repositories;

namespace JobMaster.NatJetStream.Agents;

internal class NatJetStreamJobsDispatcherRepository : AgentJobsDispatcherRepository<NatJetStreamRawMessagesDispatcherRepository, NatJetStreamRawMessagesDispatcherRepository>
{
    public NatJetStreamJobsDispatcherRepository(
        JobMasterClusterConnectionConfig connectionConfig,
        IMasterClusterConfigurationService masterClusterConfigurationService,
        IJobMasterLogger jobMasterLogger,
        NatJetStreamRawMessagesDispatcherRepository savePendingRepository,
        NatJetStreamRawMessagesDispatcherRepository processingRepository)
        : base(connectionConfig, masterClusterConfigurationService, savePendingRepository, processingRepository, jobMasterLogger)
    {
    }

    public override string AgentRepoTypeId => NatJetStreamConstants.RepositoryTypeId;
}
