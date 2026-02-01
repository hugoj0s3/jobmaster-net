using JobMaster.Abstractions;
using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Abstractions.Models;
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
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Ioc.Setup;

internal class ClusterConfigBuilder : IClusterConfigSelector
{
    internal readonly ClusterDefinition clusterDefinition = new();
    
    private readonly IServiceCollection services;

    public ClusterConfigBuilder(string? clusterId, IServiceCollection serviceProvider)
    {
        this.clusterDefinition.ClusterId = clusterId;
        this.services = serviceProvider;
    }

    public IClusterConfigSelector EnableMirrorLog(Action<LogItem> mirrorLog)
    {
        clusterDefinition.MirrorLog = mirrorLog;
        return this;
    }

    public void Finish()
    {
        if (string.IsNullOrWhiteSpace(clusterDefinition.ClusterId))
        {
            throw new ArgumentException("ClusterId must be defined via AddJobMasterCluster(name, ...) or ClusterId(...)");
        }

        if (string.IsNullOrWhiteSpace(clusterDefinition.RepoType))
        {
            throw new ArgumentException("ClusterRepoType must be defined via ClusterRepoType(...)");
        }

        if (string.IsNullOrWhiteSpace(clusterDefinition.ConnString))
        {
            throw new ArgumentException("ClusterConnString must be defined via ClusterConnString(...)");
        }

        var finalClusterId = clusterDefinition.ClusterId!;
        var finalClusterRepoType = clusterDefinition.RepoType!;
        var finalClusterConnString = clusterDefinition.ConnString!;
        
        BootstrapBlueprintDefinitions.Clusters.Add(clusterDefinition);
        
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
            finalClusterId, 
            finalClusterRepoType, 
            finalClusterConnString, 
            isDefault: clusterDefinition.IsDefault, 
            runtimeDbOperationThrottleLimit: clusterDefinition.RuntimeDbOperationThrottleLimit);

        clusterCnnConfig.SetMirrorLog(clusterDefinition.MirrorLog);

        // Apply cluster custom connection config (optional)
        clusterCnnConfig.SetJobMasterConfigDictionary(clusterDefinition.AdditionalConnConfig ?? new JobMasterConfigDictionary());

        // Register agents
        foreach (var agentConnDefinition in clusterDefinition.AgentConnections)
        {
            if (string.IsNullOrWhiteSpace(agentConnDefinition.AgentConnectionName))
            {
                throw new ArgumentException("AgentConnectionName must be defined via AgentConnectionName(...)");
            }

            if (string.IsNullOrWhiteSpace(agentConnDefinition.AgentConnString))
            {
                throw new ArgumentException("AgentConnString must be defined via AgentConnString(...)");
            }
            
            if (string.IsNullOrWhiteSpace(agentConnDefinition.AgentRepoType))
            {
                throw new ArgumentException("AgentRepoType must be defined via AgentRepoType(...)");
            }
            
            clusterCnnConfig.AddAgentConnectionString(
                agentConnDefinition.AgentConnectionName, 
                agentConnDefinition.AgentConnString ?? string.Empty, 
                agentConnDefinition.AgentRepoType!, 
                agentConnDefinition.AgentAdditionalConnConfig, 
                agentConnDefinition.RuntimeDbOperationThrottleLimit);
        }
        
        if (clusterDefinition.IsStandalone)
        {
            clusterCnnConfig.AddStandaloneAgentConnectionString();
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
        
        JobMasterIocRegistrationAttribute.RegisterAllForMaster(clusterServiceRegistration, finalClusterRepoType, finalClusterId!);
        
        var agentRepoTypes = clusterDefinition.AgentConnections.Select(a => a.AgentRepoType)!.Distinct<string>().ToList();
        agentRepoTypes.Add(finalClusterRepoType); // for standalone agents
        
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
        this.clusterDefinition.IsDefault = true;
        return this;
    }

    public IClusterConfigSelector ClusterId(string clusterId)
    {
        this.clusterDefinition.ClusterId = clusterId;
        return this;
    }

    public IClusterConfigSelector ClusterRepoType(string repoType)
    {
        this.clusterDefinition.RepoType = repoType;
        return this;
    }

    public IClusterConfigSelector ClusterConnString(string connString)
    {
        this.clusterDefinition.ConnString = connString;
        return this;
    }

    public IClusterConfigSelector ClusterDefaultJobTimeout(TimeSpan defaultJobTimeout)
    {
        this.clusterDefinition.DefaultJobTimeout = defaultJobTimeout;
        return this;
    }

    public IClusterConfigSelector ClusterTransientThreshold(TimeSpan transientThreshold)
    {
        this.clusterDefinition.TransientThreshold = transientThreshold;
        return this;
    }

    public IClusterConfigSelector ClusterDefaultMaxRetryCount(int defaultMaxRetryCount)
    {
        this.clusterDefinition.DefaultMaxRetryCount = defaultMaxRetryCount;
        return this;
    }

    public IClusterConfigSelector ClusterMaxMessageByteSize(int maxMessageByteSize)
    {
        this.clusterDefinition.MaxMessageByteSize = maxMessageByteSize;
        return this;
    }

    public IClusterConfigSelector ClusterIanaTimeZoneId(string ianaTimeZoneId)
    {
        this.clusterDefinition.IanaTimeZoneId = ianaTimeZoneId;
        return this;
    }

    public IClusterConfigSelector ClusterRuntimeDbOperationThrottleLimit(int runtimeDbOperationThrottleLimit)
    {
        this.clusterDefinition.RuntimeDbOperationThrottleLimit = runtimeDbOperationThrottleLimit;
        return this;
    }

    public IClusterConfigSelector ClusterAdditionalConfig(JobMasterConfigDictionary additionalConfig)
    {
        this.clusterDefinition.AdditionalConfig = additionalConfig;
        return this;
    }

    public IClusterConfigSelector ClusterAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig)
    {
        this.clusterDefinition.AdditionalConnConfig = additionalConnConfig;
        return this;
    }
    
    public IClusterConfigSelector AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value)
    {
        if (this.clusterDefinition.AdditionalConnConfig is null)
        {
            this.clusterDefinition.AdditionalConnConfig = new JobMasterConfigDictionary();
        }
        
        this.clusterDefinition.AdditionalConnConfig!.SetValue(namespaceKey, key, value);
        return this;
    }
    
    public IClusterConfigSelector AppendAdditionalConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value)
    {
        if (this.clusterDefinition.AdditionalConnConfig is null)
        {
            this.clusterDefinition.AdditionalConnConfig = new JobMasterConfigDictionary();
        }
        
        this.clusterDefinition.AdditionalConnConfig.SetValue(namespaceKey, key, value);
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

    public IClusterStandaloneConfigSelector UseStandaloneCluster()
    {
        clusterDefinition.IsStandalone = true;
        return new ClusterStandaloneConfigBuilder(clusterDefinition);
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

internal class ClusterStandaloneConfigBuilder : IClusterStandaloneConfigSelector
{
    private readonly ClusterDefinition clusterDefinition;

    public ClusterStandaloneConfigBuilder(ClusterDefinition clusterDefinition)
    {
        this.clusterDefinition = clusterDefinition;
        clusterDefinition.IsStandalone = true;
    }

    public IClusterStandaloneConfigSelector SetAsDefault()
    {
        clusterDefinition.IsDefault = true;
        return this;
    }

    public IClusterStandaloneConfigSelector ClusterId(string clusterId)
    {
        clusterDefinition.ClusterId = clusterId;
        return this;
    }

    public IClusterStandaloneConfigSelector ClusterDefaultJobTimeout(TimeSpan defaultJobTimeout)
    {
       clusterDefinition.DefaultJobTimeout = defaultJobTimeout;
       return this;
    }

    public IClusterStandaloneConfigSelector ClusterTransientThreshold(TimeSpan transientThreshold)
    {
        clusterDefinition.TransientThreshold = transientThreshold;
        return this;
    }

    public IClusterStandaloneConfigSelector ClusterDefaultMaxRetryCount(int defaultMaxRetryCount)
    {
        clusterDefinition.DefaultMaxRetryCount = defaultMaxRetryCount;
        return this;
    }

    public IClusterStandaloneConfigSelector ClusterMaxMessageByteSize(int maxMessageByteSize)
    {
        clusterDefinition.MaxMessageByteSize = maxMessageByteSize;
        return this;
    }

    public IClusterStandaloneConfigSelector ClusterIanaTimeZoneId(string ianaTimeZoneId)
    {
        clusterDefinition.IanaTimeZoneId = ianaTimeZoneId;
        return this;
    }

    public IClusterStandaloneConfigSelector AddWorker(
        string? workerName = null, 
        int batchSize = 250)
    {
        var def = new WorkerDefinition
        {
            WorkerName = workerName ?? string.Empty,
            BatchSize = batchSize,
            AgentConnectionName = JobMasterConstants.StandaloneAgentConnName,
        };
        clusterDefinition.Workers.Add(def);
        return this;
    }

    public IClusterStandaloneConfigSelector ClusterMode(ClusterMode mode)
    {
        clusterDefinition.ClusterMode = mode;
        return this;
    }

    public IClusterStandaloneConfigSelector ClusterConnString(string connString)
    {
        clusterDefinition.ConnString = connString;
        return  this;
    }

    public IClusterStandaloneConfigSelector ClusterRepoType(string repoType)
    {
        clusterDefinition.RepoType = repoType;
        return this;
    }
    
    public IClusterStandaloneConfigSelector ClusterAdditionalConfig(JobMasterConfigDictionary additionalConfig)
    {
        clusterDefinition.AdditionalConfig = additionalConfig;
        return this;
    }
}
