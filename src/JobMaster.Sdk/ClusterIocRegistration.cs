using JobMaster.Contracts;
using JobMaster.Contracts.Extensions;
using JobMaster.Sdk.Background;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Ioc;
using JobMaster.Sdk.Contracts.Ioc.Definitions;
using JobMaster.Sdk.Contracts.Ioc.Markups;
using JobMaster.Sdk.Contracts.Repositories.Agent;
using JobMaster.Sdk.Ioc;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Sdk;

public class ClusterIocRegistration : IClusterIocRegistration
{
    private readonly IServiceCollection mainServices;
    public string ClusterId { get; }
    public IServiceCollection ClusterServices { get; }
    
    public ClusterIocRegistration(string clusterId, IServiceCollection mainServices)
    {
        this.ClusterId = clusterId;

        ClusterServices = new ServiceCollection();
        this.mainServices = mainServices;
        
        ClusterServices.AddSingleton<JobMasterClusterConnectionConfig>(sp => JobMasterClusterConnectionConfig.Get(ClusterId, includeInactive: true));
        ClusterServices.AddSingleton<IJobMasterScheduler>(ObjectExtensions.NotNull<IJobMasterScheduler>(BootstrapBlueprintDefinitions.JobMasterScheduler));
        ClusterServices.AddSingleton<IJobMasterRuntime>(ObjectExtensions.NotNull<IJobMasterRuntime>(BootstrapBlueprintDefinitions.JobMasterRuntime));
    }
    
    public IJobMasterClusterAwareComponentFactory BuildFactory()
    {
        var serviceProvider = ClusterServices.BuildServiceProvider();
            
        var clusterComponentFactory = new JobMasterClusterAwareComponentFactory(ClusterId, serviceProvider);
        
        mainServices.AddKeyedSingleton<IJobMasterClusterAwareComponentFactory>(ClusterServiceKeys.GetClusterServiceProviderKey(ClusterId), clusterComponentFactory);
        mainServices.AddSingleton<IJobMasterScheduler>(ObjectExtensions.NotNull<IJobMasterScheduler>(BootstrapBlueprintDefinitions.JobMasterScheduler));
        return clusterComponentFactory;
    }
    
    public void AddJobMasterComponent<TService, TImplementation>() 
        where TService : class, IJobMasterClusterAwareComponent
        where TImplementation : class, TService
    {
        ClusterServices.AddSingleton<TService, TImplementation>();
    }
    
    public void AddJobMasterComponent<TService>(TService instance) 
        where TService : class, IJobMasterClusterAwareComponent
    {
        ClusterServices.AddSingleton(instance);
    }
    
    public void AddJobMasterComponent<TService>(Func<IServiceProvider, TService> factory) 
        where TService : class, IJobMasterClusterAwareComponent
    {
        ClusterServices.AddSingleton(factory);
    }
    
    public void AddRepositoryDispatcher<TRepositoryDispatcher, TSavePendingRepository, TProcessingRepository>(string repositoryTypeId) 
        where TRepositoryDispatcher : class, IAgentJobsDispatcherRepository<TSavePendingRepository, TProcessingRepository>
        where TSavePendingRepository : class, IAgentRawMessagesDispatcherRepository
        where TProcessingRepository : class, IAgentRawMessagesDispatcherRepository
    {
        ClusterServices.AddKeyedTransient<IAgentRawMessagesDispatcherRepository, TSavePendingRepository>(
            ClusterServiceKeys.GetAgentRawJobsDispatcherSavingKey(repositoryTypeId));
        ClusterServices.AddKeyedTransient<IAgentRawMessagesDispatcherRepository, TProcessingRepository>(
            ClusterServiceKeys.GetAgentRawJobsDispatcherProcessingKey(repositoryTypeId));
        
        ClusterServices.AddTransient<TSavePendingRepository>();
        ClusterServices.AddTransient<TProcessingRepository>();
        
        ClusterServices.AddKeyedTransient<IAgentJobsDispatcherRepository, TRepositoryDispatcher>(
            ClusterServiceKeys.GetAgentJobsDispatcherKey(repositoryTypeId));
    }
    
    public void AddRepositoryDispatcher<TRepositoryDispatcher>(string repositoryTypeId) 
        where TRepositoryDispatcher : class, IAgentJobsDispatcherRepository
    {
        ClusterServices.AddKeyedTransient<IAgentJobsDispatcherRepository, TRepositoryDispatcher>(
            ClusterServiceKeys.GetAgentJobsDispatcherKey(repositoryTypeId));
    }
    
    /// <summary>
    /// Registers a BucketAwareRunner (like IDrainJobsRunner) using the Factory Delegate pattern.
    /// This allows injecting the 'JobMasterBackgroundAgentWorker' at runtime while resolving other dependencies from DI.
    /// </summary>
    public void AddBucketAwareRunner<TRunner, TImplementation>(string repositoryTypeId)
        where TRunner : class, IBucketAwareRunner
        where TImplementation : class, TRunner
    {
        // We register a Func<Worker, TRunner> keyed by the repo type ID.
        // When resolved, this func uses ActivatorUtilities to create the TImplementation,
        // injecting the 'worker' passed at runtime and resolving everything else (Loggers, Configs, Repos) from the container.
        ClusterServices.AddKeyedTransient<Func<JobMasterBackgroundAgentWorker, TRunner>>(
            ClusterServiceKeys.GetAgentJobsDispatcherKey(repositoryTypeId),
            (sp, key) => (worker) => ActivatorUtilities.CreateInstance<TImplementation>(sp, worker)
        );

        // Also register the interface-based delegate signature since callers request Func<IJobMasterBackgroundAgentWorker, TRunner>
        ClusterServices.AddKeyedTransient<Func<IJobMasterBackgroundAgentWorker, TRunner>>(
            ClusterServiceKeys.GetAgentJobsDispatcherKey(repositoryTypeId),
            (sp, key) => (worker) => ActivatorUtilities.CreateInstance<TImplementation>(sp, worker)
        );
    }
}