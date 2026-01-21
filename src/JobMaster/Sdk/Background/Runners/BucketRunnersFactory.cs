using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Background.Runners.DrainRunners;
using JobMaster.Sdk.Background.Runners.JobsExecution;
using JobMaster.Sdk.Background.Runners.SavePendingJobs;
using JobMaster.Sdk.Background.Runners.SavePendingRecurringSchedule;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Background.Runners;

internal class BucketRunnersFactory : JobMasterClusterAwareComponent, IBucketRunnersFactory
{
    private readonly IAgentJobsDispatcherRepositoryFactory agentJobsDispatcherRepositoryFactory;
    private IJobMasterClusterAwareComponentFactory AwareComponentFactory => JobMasterClusterAwareComponentFactories.GetFactory(this.ClusterConnConfig.ClusterId);
    
    public BucketRunnersFactory(
        IAgentJobsDispatcherRepositoryFactory agentJobsDispatcherRepositoryFactory, 
        JobMasterClusterConnectionConfig clusterConnectionConfig) : base(clusterConnectionConfig)
    {
        this.agentJobsDispatcherRepositoryFactory = agentJobsDispatcherRepositoryFactory;
    }
    
    
    
    public IDrainJobsRunner NewDrainJobsRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker, 
        AgentConnectionId agentConnectionId)
    {
        return NewDrainSavePendingJobsRunner(backgroundAgentWorker, agentConnectionId);
    }

    public IDrainSavePendingJobsRunner NewDrainSavePendingJobsRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        AgentConnectionId agentConnectionId)
    {
        var agentJobsDispatcherRepository = agentJobsDispatcherRepositoryFactory.GetRepository(agentConnectionId);

        if (!agentJobsDispatcherRepository.IsAutoDequeueForSaving)
        {
            return new ManualDrainJobsRunner(backgroundAgentWorker);
        }

        var agentCnnConfig = JobMasterClusterConnectionConfig.TryGet(this.ClusterConnConfig.ClusterId)
            ?.TryGetAgentConnectionConfig(agentConnectionId.IdValue);
        if (agentCnnConfig is null)
        {
            throw new InvalidOperationException($"Agent connection {agentConnectionId} not found");
        }

        return this.AwareComponentFactory.GetBucketAwareRunner<IDrainSavePendingJobsRunner>(agentCnnConfig.RepositoryTypeId, backgroundAgentWorker);
    }

    public IDrainProcessingJobsRunner NewDrainProcessingJobsRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        AgentConnectionId agentConnectionId)
    {
        var agentJobsDispatcherRepository = agentJobsDispatcherRepositoryFactory.GetRepository(agentConnectionId);

        if (!agentJobsDispatcherRepository.IsAutoDequeueForProcessing)
        {
            return new ManualDrainProcessingJobsRunner(backgroundAgentWorker);
        }

        var agentCnnConfig = JobMasterClusterConnectionConfig.TryGet(this.ClusterConnConfig.ClusterId)
            ?.TryGetAgentConnectionConfig(agentConnectionId.IdValue);
        if (agentCnnConfig is null)
        {
            throw new InvalidOperationException($"Agent connection {agentConnectionId} not found");
        }

        return this.AwareComponentFactory.GetBucketAwareRunner<IDrainProcessingJobsRunner>(agentCnnConfig.RepositoryTypeId, backgroundAgentWorker);
    }

    public IDrainSavePendingRecurringScheduleRunner NewDrainSavePendingRecurringScheduleRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        AgentConnectionId agentConnectionId)
    {
        var agentJobsDispatcherRepository = agentJobsDispatcherRepositoryFactory.GetRepository(agentConnectionId);

        if (!agentJobsDispatcherRepository.IsAutoDequeueForSaving)
        {
            return new ManualDrainSavePendingRecurringScheduleRunner(backgroundAgentWorker);
        }

        var agentCnnConfig = JobMasterClusterConnectionConfig.TryGet(this.ClusterConnConfig.ClusterId)
            ?.TryGetAgentConnectionConfig(agentConnectionId.IdValue);
        if (agentCnnConfig is null)
        {
            throw new InvalidOperationException($"Agent connection {agentConnectionId} not found");
        }

        return this.AwareComponentFactory.GetBucketAwareRunner<IDrainSavePendingRecurringScheduleRunner>(agentCnnConfig.RepositoryTypeId, backgroundAgentWorker);
    }

    public IJobsExecutionRunner NewJobsExecutionRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        AgentConnectionId agentConnectionId)
    {
        var agentJobsDispatcherRepository = agentJobsDispatcherRepositoryFactory.GetRepository(agentConnectionId);
        
        if (!agentJobsDispatcherRepository.IsAutoDequeueForProcessing)
        {
            return new ManualJobsExecutionRunner(backgroundAgentWorker);
        }
        
        var agentCnnConfig = JobMasterClusterConnectionConfig.TryGet(this.ClusterConnConfig.ClusterId)?.TryGetAgentConnectionConfig(agentConnectionId.IdValue);
        if (agentCnnConfig is null)
        {
            throw new InvalidOperationException($"Agent connection {agentConnectionId} not found");
        }
        
        var jobsExecutionRunner = this.AwareComponentFactory.GetBucketAwareRunner<IJobsExecutionRunner>(agentCnnConfig.RepositoryTypeId, backgroundAgentWorker);
        return jobsExecutionRunner;
    }
    
    public ISavePendingJobsRunner NewSavePendingJobsRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        AgentConnectionId agentConnectionId)
    {
        var agentJobsDispatcherRepository = agentJobsDispatcherRepositoryFactory.GetRepository(agentConnectionId);
        
        if (!agentJobsDispatcherRepository.IsAutoDequeueForSaving)
        {
            return new ManualSavePendingJobsRunner(backgroundAgentWorker);
        }
        
        var agentCnnConfig = JobMasterClusterConnectionConfig.TryGet(this.ClusterConnConfig.ClusterId)?.TryGetAgentConnectionConfig(agentConnectionId.IdValue);
        if (agentCnnConfig is null)
        {
            throw new InvalidOperationException($"Agent connection {agentConnectionId} not found");
        }
        
        var savePendingJobsRunner = this.AwareComponentFactory.GetBucketAwareRunner<ISavePendingJobsRunner>(agentCnnConfig.RepositoryTypeId, backgroundAgentWorker);
        return savePendingJobsRunner;
    }

    public ISaveRecurringSchedulerRunner NewSaveRecurringSchedulerRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker, AgentConnectionId agentConnectionId)
    {
        var agentJobsDispatcherRepository = agentJobsDispatcherRepositoryFactory.GetRepository(agentConnectionId);
        
        if (!agentJobsDispatcherRepository.IsAutoDequeueForSaving)
        {
            return new ManualSaveRecurringScheduleRunner(backgroundAgentWorker);
        }
        
        var agentCnnConfig = JobMasterClusterConnectionConfig.TryGet(this.ClusterConnConfig.ClusterId)?.TryGetAgentConnectionConfig(agentConnectionId.IdValue);
        if (agentCnnConfig is null)
        {
            throw new InvalidOperationException($"Agent connection {agentConnectionId} not found");
        }
        
        var savePendingJobsRunner = this.AwareComponentFactory.GetBucketAwareRunner<ISaveRecurringSchedulerRunner>(agentCnnConfig.RepositoryTypeId, backgroundAgentWorker);
        return savePendingJobsRunner;
    }
}