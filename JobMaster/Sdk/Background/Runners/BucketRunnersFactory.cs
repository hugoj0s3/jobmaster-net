using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Background.Runners.DrainRunners;
using JobMaster.Sdk.Background.Runners.JobsExecution;
using JobMaster.Sdk.Background.Runners.SavePendingJobs;
using JobMaster.Sdk.Background.Runners.SavePendingRecurringSchedule;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Abstractions.Models;

namespace JobMaster.Sdk.Background.Runners;

internal class BucketRunnersFactory : JobMasterClusterAwareComponent, IBucketRunnersFactory
{
    private readonly IAgentComponentFactory agentComponentFactory;
    private IJobMasterClusterAwareComponentFactory AwareComponentFactory => JobMasterClusterAwareComponentFactories.GetFactory(this.ClusterConnConfig.ClusterId);
    
    public BucketRunnersFactory(
        IAgentComponentFactory agentComponentFactory, 
        JobMasterClusterConnectionConfig clusterConnectionConfig) : base(clusterConnectionConfig)
    {
        this.agentComponentFactory = agentComponentFactory;
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
        var agentJobsDispatcherRepository = agentComponentFactory.GetRepository(agentConnectionId);

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
        var agentJobsDispatcherRepository = agentComponentFactory.GetRepository(agentConnectionId);

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
        var agentJobsDispatcherRepository = agentComponentFactory.GetRepository(agentConnectionId);

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
        AgentConnectionId agentConnectionId,
        string bucketId,
        JobMasterPriority priority)
    {
        var agentJobsDispatcherRepository = agentComponentFactory.GetRepository(agentConnectionId);
        IJobsExecutionRunner jobsExecutionRunner;
        if (!agentJobsDispatcherRepository.IsAutoDequeueForProcessing)
        {
            var dispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
            var standardSource = new StandardBucketJobsOnboardingSource(dispatcherService, agentConnectionId, bucketId);
            jobsExecutionRunner = ManualJobsExecutionRunner.Create(backgroundAgentWorker, standardSource);
        }
        else
        { 
            var agentCnnConfig = JobMasterClusterConnectionConfig.TryGet(this.ClusterConnConfig.ClusterId)?.TryGetAgentConnectionConfig(agentConnectionId.IdValue);
            if (agentCnnConfig is null)
            {
                throw new InvalidOperationException($"Agent connection {agentConnectionId} not found");
            }
        
            jobsExecutionRunner = this.AwareComponentFactory.GetBucketAwareRunner<IJobsExecutionRunner>(agentCnnConfig.RepositoryTypeId, backgroundAgentWorker);

        }
        
        jobsExecutionRunner.DefineBucketId(bucketId, priority);
        return jobsExecutionRunner;
    }
    
    public ISavePendingJobsRunner NewSavePendingJobsRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        AgentConnectionId agentConnectionId)
    {
        var agentJobsDispatcherRepository = agentComponentFactory.GetRepository(agentConnectionId);
        
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
        var agentJobsDispatcherRepository = agentComponentFactory.GetRepository(agentConnectionId);
        
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
