using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Agents;

namespace JobMaster.Sdk.Abstractions.Background;

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