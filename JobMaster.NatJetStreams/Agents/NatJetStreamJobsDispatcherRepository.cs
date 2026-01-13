using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Repositories;

namespace JobMaster.NatJetStreams.Agents;

internal class NatJetStreamJobsDispatcherRepository : AgentJobsDispatcherRepository<NatJetStreamsRawMessagesDispatcherRepository, NatJetStreamsRawMessagesDispatcherRepository>
{
    public NatJetStreamJobsDispatcherRepository(
        JobMasterClusterConnectionConfig connectionConfig,
        IMasterClusterConfigurationService masterClusterConfigurationService,
        IJobMasterLogger jobMasterLogger,
        NatJetStreamsRawMessagesDispatcherRepository savePendingRepository,
        NatJetStreamsRawMessagesDispatcherRepository processingRepository)
        : base(connectionConfig, masterClusterConfigurationService, savePendingRepository, processingRepository, jobMasterLogger)
    {
    }

    public override string AgentRepoTypeId => NatJetStreamConstants.RepositoryTypeId;
}
