using JobMaster.Abstractions;
using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Abstractions.Models;
using JobMaster.Internals;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.BucketSelector;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.LocalCache;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Services;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.BucketSelector;
using JobMaster.Sdk.Ioc.Setup.Selectors;
using JobMaster.Sdk.LocalCache;
using JobMaster.Sdk.Repositories;
using JobMaster.Sdk.Services;
using JobMaster.Sdk.Services.Agents;
using JobMaster.Sdk.Services.Master;
using Microsoft.Extensions.DependencyInjection;
using JobMaster.Sdk.Background;
using JobMaster.Sdk.Background.Runners;

namespace JobMaster.Sdk.Ioc.Setup;

internal class ClusterConfigBuilder : IClusterConfigSelectorAdvanced
{
    private string? clusterId;
    
    private string? clusterRepoType;
    private string? clusterConnString;
    private JobMasterConfigDictionary clusterAdditionalConnConfig = new();
    
    private int? clusterDefaultMaxRetryCount;
    private TimeSpan? clusterDefaultJobTimeout;
    private int? clusterMaxMessageByteSize;
    private string? clusterIanaTimeZoneId;
    private bool? clusterIsDefault;
    private JobMasterConfigDictionary? clusterAdditionalConfig;
    
    internal readonly ClusterDefinition clusterDefinition = new();
    
    private readonly IServiceCollection services;
    private TimeSpan clusterTransientThreshold;
    private int? clusterRuntimeDbOperationThrottleLimit;

    public ClusterConfigBuilder(string? clusterId, IServiceCollection serviceProvider)
    {
        this.clusterId = clusterId;
        this.services = serviceProvider;
    }

    public IClusterConfigSelector EnableMirrorLog(Action<LogItem> mirrorLog)
    {
        clusterDefinition.MirrorLog = mirrorLog;
        return this;
    }

    public void Finish()
    {
        var finalClusterId = clusterId;
        if (string.IsNullOrWhiteSpace(finalClusterId))
        {
            throw new ArgumentException("ClusterId must be defined via AddJobMasterCluster(name, ...) or ClusterId(...)");
        }

        if (string.IsNullOrWhiteSpace(clusterRepoType))
        {
            throw new ArgumentException("ClusterRepoType must be defined via ClusterRepoType(...)");
        }

        if (string.IsNullOrWhiteSpace(clusterConnString))
        {
            throw new ArgumentException("ClusterConnString must be defined via ClusterConnString(...)");
        }

        clusterDefinition.ClusterId = finalClusterId!;
        clusterDefinition.RepoType = clusterRepoType!;
        clusterDefinition.ConnString = clusterConnString!;
        
        clusterDefinition.AdditionalConnConfig = clusterAdditionalConnConfig;
        clusterDefinition.DefaultMaxRetryCount = clusterDefaultMaxRetryCount;
        clusterDefinition.TransientThreshold = clusterTransientThreshold;
        clusterDefinition.DefaultJobTimeout = clusterDefaultJobTimeout;
        clusterDefinition.MaxMessageByteSize = clusterMaxMessageByteSize;
        clusterDefinition.IanaTimeZoneId = clusterIanaTimeZoneId;
        clusterDefinition.AdditionalConfig = clusterAdditionalConfig;
        clusterDefinition.IsDefault = clusterIsDefault ?? false;
        clusterDefinition.RuntimeDbOperationThrottleLimit = clusterRuntimeDbOperationThrottleLimit;
        
        BootstrapBlueprintDefinitions.Clusters.Add(clusterDefinition);
        // Register workers
        foreach (var w in clusterDefinition.Workers)
        {
            w.ClusterId = finalClusterId!;
        }

        foreach (var a in clusterDefinition.AgentConnections)
        {
            a.ClusterId = finalClusterId!;
        }

        // Create or get ClusterConnectionConfig
        var clusterCnnConfig = JobMasterClusterConnectionConfig.Create(
            finalClusterId!, clusterRepoType!, 
            clusterConnString!, 
            isDefault: this.clusterIsDefault ?? false, 
            runtimeDbOperationThrottleLimit: this.clusterRuntimeDbOperationThrottleLimit);

        clusterCnnConfig.SetMirrorLog(clusterDefinition.MirrorLog);

        // Apply cluster custom connection config (optional)
        clusterCnnConfig.SetJobMasterConfigDictionary(clusterAdditionalConnConfig);

        // Register agents
        foreach (var a in clusterDefinition.AgentConnections)
        {
            var repoType = string.IsNullOrWhiteSpace(a.AgentRepoType) ? clusterRepoType! : a.AgentRepoType!;
            var additionalConnConfig = a.AgentAdditionalConnConfig;
            clusterCnnConfig.AddAgentConnectionString(a.AgentConnectionName, a.AgentConnString ?? string.Empty, repoType, additionalConnConfig, a.RuntimeDbOperationThrottleLimit);
        }

        services.AddSingleton<IJobMasterScheduler>(BootstrapBlueprintDefinitions.JobMasterScheduler!);
        
        var clusterServiceRegistration = new ClusterIocRegistration(finalClusterId!, services);
        clusterServiceRegistration.ClusterServices.AddSingleton<IBucketSelectorAlgorithm, RandomBucketSelectorAlgorithm>();
        clusterServiceRegistration.ClusterServices.AddSingleton<IJobMasterScheduler>(BootstrapBlueprintDefinitions.JobMasterScheduler!);
        clusterServiceRegistration.ClusterServices.AddSingleton<IJobMasterInMemoryCache, JobMasterInMemoryCache>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterAgentWorkersService, MasterAgentWorkersService>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterJobsService, MasterJobsService>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterChangesSentinelService, MasterChangesSentinelService>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterDistributedLockerService, MasterDistributedLockerService>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterBucketsService, MasterBucketsService>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterClusterConfigurationService, MasterClusterConfigurationService>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterHeartbeatService, MasterHeartbeatService>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterRecurringSchedulesService, MasterRecurringSchedulesService>();
        clusterServiceRegistration.AddJobMasterComponent<IAgentJobsDispatcherService, AgentJobsDispatcherService>();
        clusterServiceRegistration.AddJobMasterComponent<IJobMasterSchedulerClusterAware, JobMasterSchedulerClusterAware>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterAgentWorkersService, MasterAgentWorkersService>();
        clusterServiceRegistration.AddJobMasterComponent<IAgentJobsDispatcherRepositoryFactory, AgentJobsDispatcherRepositoryFactory>();
        clusterServiceRegistration.AddJobMasterComponent<IRecurringSchedulePlanner, RecurringSchedulePlanner>();
        clusterServiceRegistration.AddJobMasterComponent<IJobMasterLogger, JobMasterLogger>();
        clusterServiceRegistration.AddJobMasterComponent<IBucketRunnersFactory, BucketRunnersFactory>();
        clusterServiceRegistration.AddJobMasterComponent<IWorkerClusterOperations, WorkerClusterOperations>();
        
        JobMasterIocRegistrationAttribute.RegisterAllForMaster(clusterServiceRegistration, this.clusterRepoType!, finalClusterId!);
        
        var agentRepoTypes = clusterDefinition.AgentConnections.Select(a => a.AgentRepoType)!.Distinct<string>().ToList();
        foreach (var agentRepoType in agentRepoTypes)
        {
            if (string.IsNullOrEmpty(agentRepoType))
                continue;
            
            JobMasterIocRegistrationAttribute.RegisterAllForAgent(clusterServiceRegistration, agentRepoType, finalClusterId!);
        }
        
        // Register runtime setup. get all impl and register for IJobMasterRuntimeSetup
        var assemblies = AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
            .ToList();

        var runtimeSetupTypes = assemblies
            .SelectMany(a => a.GetTypes())
            .Where(t => typeof(IJobMasterRuntimeSetup).IsAssignableFrom(t) && 
                        !t.IsInterface && 
                        !t.IsAbstract)
            .ToList();

        foreach (var runtimeSetupType in runtimeSetupTypes)
        {
            services.AddScoped(typeof(IJobMasterRuntimeSetup), runtimeSetupType);
            clusterServiceRegistration.ClusterServices.AddScoped(typeof(IJobMasterRuntimeSetup), runtimeSetupType);
        }
        
        // Automatically register all IJobHandler implementations
        var handlerTypes = assemblies
            .SelectMany(a => a.GetTypes())
            .Where(t => typeof(IJobHandler).IsAssignableFrom(t) && 
                        !t.IsInterface && 
                        !t.IsAbstract)
            .ToList();
        
        foreach (var handlerType in handlerTypes)
        {
            services.AddScoped(handlerType);
        }
        
        var factory = clusterServiceRegistration.BuildFactory();
        JobMasterClusterAwareComponentFactories.AddFactory(finalClusterId!, factory);
    }

    // IClusterConfigSelector implementation
    public IClusterConfigSelector SetAsDefault()
    {
        this.clusterIsDefault = true;
        return this;
    }

    public IClusterConfigSelector ClusterId(string clusterId)
    {
        this.clusterId = clusterId;
        return this;
    }

    public IClusterConfigSelector ClusterRepoType(string repoType)
    {
        this.clusterRepoType = repoType;
        return this;
    }

    public IClusterConfigSelector ClusterConnString(string connString)
    {
        this.clusterConnString = connString;
        return this;
    }

    public IClusterConfigSelector ClusterDefaultJobTimeout(TimeSpan defaultJobTimeout)
    {
        this.clusterDefaultJobTimeout = defaultJobTimeout;
        return this;
    }

    public IClusterConfigSelector ClusterTransientThreshold(TimeSpan transientThreshold)
    {
        this.clusterTransientThreshold = transientThreshold;
        return this;
    }

    public IClusterConfigSelector ClusterDefaultMaxRetryCount(int defaultMaxRetryCount)
    {
        this.clusterDefaultMaxRetryCount = defaultMaxRetryCount;
        return this;
    }

    public IClusterConfigSelector ClusterMaxMessageByteSize(int maxMessageByteSize)
    {
        this.clusterMaxMessageByteSize = maxMessageByteSize;
        return this;
    }

    public IClusterConfigSelector ClusterIanaTimeZoneId(string ianaTimeZoneId)
    {
        this.clusterIanaTimeZoneId = ianaTimeZoneId;
        return this;
    }

    public IClusterConfigSelector ClusterRuntimeDbOperationThrottleLimit(int runtimeDbOperationThrottleLimit)
    {
        this.clusterRuntimeDbOperationThrottleLimit = runtimeDbOperationThrottleLimit;
        return this;
    }

    public IClusterConfigSelector ClusterAdditionalConfig(JobMasterConfigDictionary additionalConfig)
    {
        this.clusterAdditionalConfig = additionalConfig;
        return this;
    }

    public IClusterConfigSelector ClusterAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig)
    {
        this.clusterAdditionalConnConfig = additionalConnConfig;
        return this;
    }
    
    public IClusterConfigSelector AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value)
    {
        this.clusterAdditionalConnConfig.SetValue(namespaceKey, key, value);
        return this;
    }
    
    public IClusterConfigSelector AppendAdditionalConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value)
    {
        if (this.clusterAdditionalConfig is null)
        {
            this.clusterAdditionalConfig = new JobMasterConfigDictionary();
        }
        
        this.clusterAdditionalConfig.SetValue(namespaceKey, key, value);
        return this;
    }

    public IClusterConfigSelector DebugJsonlFileLogger(string filePath, int maxBufferItems = 500, TimeSpan? flushInterval = null)
    {
        clusterDefinition.MirrorLogFilePath = filePath;
        clusterDefinition.MirrorLogMaxBufferItems = maxBufferItems;
        clusterDefinition.MirrorLogFlushInterval = flushInterval;
        clusterDefinition.MirrorLog = JsonlFileLogger.LogMirror;
        return this;
    }

    public IAgentConnectionConfigSelector AddAgentConnectionConfig(
        string agentConnectionName, 
        string? repoType = null,
        string? cnnString = null)
    {
        return AddAgentConnectionConfig(agentConnectionName, repoType, cnnString, null);
    }

    public IClusterConfigSelector ClusterMode(ClusterMode mode)
    {
        clusterDefinition.ClusterMode = mode;
        return this;
    }

    public IAgentConnectionConfigSelector AddAgentConnectionConfig(string agentConnectionName, string? repoType, string? cnnString, JobMasterConfigDictionary? additionalConnConfig)
    {
        if (!JobMasterStringUtils.IsValidForSegment(agentConnectionName))
        {
            throw new ArgumentException("Invalid agentConnectionName use 0-9, Aa-Zz and - _ only");
        }
        
        var def = new AgentConnectionDefinition
        {
            AgentConnectionName = agentConnectionName,
            AgentRepoType = repoType,
            AgentConnString = cnnString,
            AgentAdditionalConnConfig = additionalConnConfig,
        };
        clusterDefinition.AgentConnections.Add(def);
        return new AgentConnectionConfigSelector(this, def);
    }

    public IAgentWorkerSelector AddWorker(
        string? workerName = null, 
        string? agentConnectionName = null, 
        int batchSize = 1000)
    {
        var def = new WorkerDefinition
        {
            AgentConnectionName = agentConnectionName ?? string.Empty,
            WorkerName = workerName ?? string.Empty,
            BatchSize = batchSize,
        };
        clusterDefinition.Workers.Add(def);
        return new AgentWorkerSelector(this, def);
    }
}