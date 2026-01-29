using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Repositories;

namespace JobMaster.NatsJetStream.Agents;

internal class NatsJetStreamJobsDispatcherRepository : AgentJobsDispatcherRepository<NatsJetStreamRawMessagesDispatcherRepository, NatsJetStreamRawMessagesDispatcherRepository>
{
    public NatsJetStreamJobsDispatcherRepository(
        JobMasterClusterConnectionConfig connectionConfig,
        IMasterClusterConfigurationService masterClusterConfigurationService,
        IJobMasterLogger jobMasterLogger,
        NatsJetStreamRawMessagesDispatcherRepository savePendingRepository,
        NatsJetStreamRawMessagesDispatcherRepository processingRepository)
        : base(connectionConfig, masterClusterConfigurationService, savePendingRepository, processingRepository, jobMasterLogger)
    {
    }

    public override string AgentRepoTypeId => NatsJetStreamConstants.RepositoryTypeId;
}
