using JobMaster.Sdk.Contracts.Background.Runners;
using JobMaster.Sdk.Contracts.Ioc.Markups;
using JobMaster.Sdk.Contracts.Models.Agents;

namespace JobMaster.Sdk.Contracts.Background;

public interface IBucketRunnersFactory : IJobMasterClusterAwareComponent
{
    IDrainJobsRunner NewDrainJobsRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        AgentConnectionId agentConnectionId);

    IDrainSavePendingJobsRunner NewDrainSavePendingJobsRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        AgentConnectionId agentConnectionId);

    IDrainProcessingJobsRunner NewDrainProcessingJobsRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        AgentConnectionId agentConnectionId);

    IDrainSavePendingRecurringScheduleRunner NewDrainSavePendingRecurringScheduleRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        AgentConnectionId agentConnectionId);
    
    IJobsExecutionRunner NewJobsExecutionRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        AgentConnectionId agentConnectionId);
    
    ISavePendingJobsRunner NewSavePendingJobsRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        AgentConnectionId agentConnectionId);
    
    ISaveRecurringSchedulerRunner NewSaveRecurringSchedulerRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        AgentConnectionId agentConnectionId);
}