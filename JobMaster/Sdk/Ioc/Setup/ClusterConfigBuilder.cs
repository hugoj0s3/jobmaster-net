using System.Globalization;
using System.Text.Json;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.BucketSelector;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
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
using JobMaster.Sdk.Ioc.Setup.Json;
using JobMaster.Sdk.Ioc.Setup.Selectors;
using JobMaster.Sdk.Repositories;
using JobMaster.Sdk.Services;
using JobMaster.Sdk.Services.Agents;
using JobMaster.Sdk.Services.Master;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using JobMaster.Sdk.Background;
using JobMaster.Sdk.Background.Runners;
using JobMaster.Sdk.Cache;
using JobMaster.Sdk.Ioc.Setup.Strategies;
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
            runtimeDbOperationThrottleLimit: clusterDefinition.RuntimeDbOperationLimit);

        clusterCnnConfig.SetMirrorLog(clusterDefinition.MirrorLog);

        if (clusterDefinition.DisabledPriorities.Any())
            clusterCnnConfig.SetDisabledPriorities(clusterDefinition.DisabledPriorities);

        foreach (var worker in clusterDefinition.Workers)
        {
            // Validate: explicit BucketQtyConfig for a disabled priority is a configuration error
            foreach (var p in clusterDefinition.DisabledPriorities)
            {
                if (worker.BucketQty.TryGetValue(p, out int explicit_qty) && explicit_qty >= 1)
                    throw new InvalidOperationException(
                        $"Worker '{worker.WorkerName}' has BucketQtyConfig({p}, {explicit_qty}) but " +
                        $"priority {p} is disabled on cluster '{finalClusterId}'. " +
                        $"Remove the BucketQtyConfig call or set it to 0.");
            }

            // Fill defaults for every priority not explicitly configured
            foreach (JobMasterPriority p in Enum.GetValues(typeof(JobMasterPriority)))
            {
                if (!worker.BucketQty.ContainsKey(p))
                    worker.BucketQty[p] = clusterDefinition.DisabledPriorities.Contains(p) ? 0 : JobMasterDefaults.Worker.BucketQtyPerPriority;
            }
        }

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
                agentConnDefinition.RuntimeDbOperationLimit);
        }
        
        if (clusterDefinition.IsStandalone == true)
        {
            clusterCnnConfig.AddStandaloneAgentConnectionString();
        }

        services.AddSingleton<IJobMasterScheduler>(BootstrapBlueprintDefinitions.JobMasterScheduler!);
        
        var clusterServiceRegistration = new ClusterIocRegistration(finalClusterId!, services);
        clusterServiceRegistration.ClusterServices.AddSingleton<IBucketSelectorAlgorithm, RandomBucketSelectorAlgorithm>();
        clusterServiceRegistration.ClusterServices.AddSingleton<IJobMasterScheduler>(BootstrapBlueprintDefinitions.JobMasterScheduler!);
        clusterServiceRegistration.ClusterServices.AddSingleton<IJobMasterInMemoryCache, JobMasterInMemoryCache>();
        clusterServiceRegistration.ClusterServices.AddSingleton<DefaultKnownExceptionIdentifierStrategy>();
        clusterServiceRegistration.AddJobMasterComponent<IKnownExceptionIdentifier, KnownExceptionIdentifier>();
        
        clusterServiceRegistration.AddJobMasterComponent<IMasterAgentWorkersService, MasterAgentWorkersService>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterJobsService, MasterJobsService>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterChangesSentinelService, MasterChangesSentinelService>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterDistributedLockerService, MasterDistributedLockerService>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterBucketsService, MasterBucketsService>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterClusterConfigurationService, MasterClusterConfigurationService>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterHeartbeatService, MasterHeartbeatService>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterRecurringSchedulesService, MasterRecurringSchedulesService>();
        clusterServiceRegistration.AddJobMasterComponent<IRecentlyInsertedRecurringScheduleQueue, RecentlyInsertedRecurringScheduleQueue>();
        clusterServiceRegistration.AddJobMasterComponent<IAgentJobsDispatcherService, AgentJobsDispatcherService>();
        clusterServiceRegistration.AddJobMasterComponent<IJobMasterSchedulerClusterAware, JobMasterSchedulerClusterAware>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterAgentWorkersService, MasterAgentWorkersService>();
        clusterServiceRegistration.AddJobMasterComponent<IAgentComponentFactory, AgentComponentFactory>();
        clusterServiceRegistration.AddJobMasterComponent<IRecurringSchedulePlanner, RecurringSchedulePlanner>();
        clusterServiceRegistration.AddJobMasterComponent<IJobMasterLogger, JobMasterLogger>();
        clusterServiceRegistration.AddJobMasterComponent<IBucketRunnersFactory, BucketRunnersFactory>();
        clusterServiceRegistration.AddJobMasterComponent<IWorkerClusterOperations, WorkerClusterOperations>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterAgentConnectionService, MasterAgentConnectionService>();
        clusterServiceRegistration.AddJobMasterComponent<IHostStatsProvider, NullHostStatsProvider>();
        clusterServiceRegistration.AddJobMasterComponent<IMasterHostService, MasterHostService>();

        JobMasterIocRegistrationAttribute.RegisterProviderExtensionsForMaster(clusterServiceRegistration, finalClusterRepoType, finalClusterId!);
        
        var agentRepoTypes = clusterDefinition.AgentConnections.Select(a => a.AgentRepoType)!.Distinct<string>().ToList();
        agentRepoTypes.Add(finalClusterRepoType); // for standalone agents
        
        foreach (var agentRepoType in agentRepoTypes)
        {
            if (string.IsNullOrEmpty(agentRepoType))
                continue;
            
            JobMasterIocRegistrationAttribute.RegisterProviderExtensionsForAgent(clusterServiceRegistration, agentRepoType, finalClusterId!);
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
        
        // Automatically register all IJobMasterHandler implementations
        var handlerTypes = assemblies
            .SelectMany(a => a.GetTypes())
            .Where(t => typeof(IJobMasterHandler).IsAssignableFrom(t) && 
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
        if (!JobMasterStringUtils.IsValidForSegment(clusterId, 25))
        {
            throw new ArgumentException($"ClusterId {clusterId} is not valid for segment. It must be between 1 and 25 characters long and contain only letters, numbers, hyphens, and underscores.");
        }
        
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

    public IClusterConfigSelector DefaultJobTimeout(TimeSpan defaultJobTimeout)
    {
        this.clusterDefinition.DefaultJobTimeout = defaultJobTimeout;
        return this;
    }

    public IClusterConfigSelector ClusterDefaultJobTimeout(TimeSpan defaultJobTimeout) => DefaultJobTimeout(defaultJobTimeout);

    public IClusterConfigSelector TransientThreshold(TimeSpan transientThreshold)
    {
        this.clusterDefinition.TransientThreshold = transientThreshold;
        return this;
    }

    public IClusterConfigSelector ClusterTransientThreshold(TimeSpan transientThreshold) => TransientThreshold(transientThreshold);

    public IClusterConfigSelector DefaultMaxRetryCount(int defaultMaxRetryCount)
    {
        this.clusterDefinition.DefaultMaxRetryCount = defaultMaxRetryCount;
        return this;
    }

    public IClusterConfigSelector ClusterDefaultMaxRetryCount(int defaultMaxRetryCount) => DefaultMaxRetryCount(defaultMaxRetryCount);

    public IClusterConfigSelector MaxMessageByteSize(int maxMessageByteSize)
    {
        this.clusterDefinition.MaxMessageByteSize = maxMessageByteSize;
        return this;
    }

    public IClusterConfigSelector ClusterMaxMessageByteSize(int maxMessageByteSize) => MaxMessageByteSize(maxMessageByteSize);

    public IClusterConfigSelector IanaTimeZoneId(string ianaTimeZoneId)
    {
        this.clusterDefinition.IanaTimeZoneId = ianaTimeZoneId;
        return this;
    }

    public IClusterConfigSelector ClusterIanaTimeZoneId(string ianaTimeZoneId) => IanaTimeZoneId(ianaTimeZoneId);

    public IClusterConfigSelector DataRetentionTtl(TimeSpan dataRetentionTtl)
    {
        this.clusterDefinition.SetDataRetentionTtl(dataRetentionTtl);
        return this;
    }

    public IClusterConfigSelector RetainDataForever()
    {
        this.clusterDefinition.DataRetentionTtl = TimeSpan.Zero;
        return this;
    }

    public IClusterConfigSelector DisablePriority(JobMasterPriority priority)
    {
        clusterDefinition.DisablePriority(priority);
        return this;
    }

    public IClusterConfigSelector ClusterRuntimeDbOperationLimit(int runtimeDbOperationThrottleLimit)
    {
        this.clusterDefinition.RuntimeDbOperationLimit = runtimeDbOperationThrottleLimit;
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

    public IClusterConfigSelector Mode(ClusterMode mode)
    {
        clusterDefinition.ClusterMode = mode;
        return this;
    }

    public IClusterConfigSelector ClusterMode(ClusterMode mode) => Mode(mode);

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
        return new AgentConnectionConfigSelector(def);
    }

    public IAgentWorkerSelector AddWorker(
        string? workerName = null,
        string? agentConnectionName = null,
        int transferBatchSize = JobMasterDefaults.Worker.TransferBatchSize,
        int bucketBufferSize = JobMasterDefaults.Worker.BucketBufferSize)
    {
        var def = new WorkerDefinition
        {
            AgentConnectionName = agentConnectionName ?? string.Empty,
            WorkerName = workerName ?? string.Empty,
            TransferBatchSize = transferBatchSize,
            BucketBufferSize = bucketBufferSize,
        };
        clusterDefinition.Workers.Add(def);
        return new AgentWorkerSelector(this, def);
    }

    public IClusterConfigSelector ConfigFromJson(IConfiguration section)
    {
        var config = section.Get<ClusterJsonConfig>()
            ?? throw new ArgumentException("Could not bind the configuration section to ClusterJsonConfig.");
        return ApplyJsonConfig(config);
    }

    public IClusterConfigSelector ConfigFromJson(string jsonOrFilePath)
    {
        var trimmed = jsonOrFilePath.TrimStart();
        bool isFilePath = !trimmed.StartsWith("{", StringComparison.Ordinal) &&
                          (jsonOrFilePath.EndsWith(".json", StringComparison.OrdinalIgnoreCase) ||
                           File.Exists(jsonOrFilePath));

        var json = isFilePath ? File.ReadAllText(jsonOrFilePath) : jsonOrFilePath;
        var config = JsonSerializer.Deserialize<ClusterJsonConfig>(json, JsonOptions)
            ?? throw new ArgumentException("Could not deserialize the JSON string to ClusterJsonConfig.");
        return ApplyJsonConfig(config);
    }

    public IClusterConfigSelector ConfigFromJson(Stream stream)
    {
        using var reader = new StreamReader(stream);
        var json = reader.ReadToEnd();
        var config = JsonSerializer.Deserialize<ClusterJsonConfig>(json, JsonOptions)
            ?? throw new ArgumentException("Could not deserialize the stream to ClusterJsonConfig.");
        return ApplyJsonConfig(config);
    }

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    private static Dictionary<string, object> ToIgnoreCase(Dictionary<string, object> source)
        => new(source, StringComparer.OrdinalIgnoreCase);

    private IClusterConfigSelector ApplyJsonConfig(ClusterJsonConfig config)
    {
        if (config.ClusterId != null) ClusterId(config.ClusterId);
        if (config.SetAsDefault) SetAsDefault();

        if (config.Mode != null)
        {
            if (!Enum.TryParse(config.Mode, ignoreCase: true, out ClusterMode mode))
                throw new ArgumentException($"Invalid ClusterMode value: '{config.Mode}'.");
            Mode(mode);
        }

        if (config.TransientThreshold != null)
            TransientThreshold(TimeSpan.Parse(config.TransientThreshold, CultureInfo.InvariantCulture));
        if (config.DefaultJobTimeout != null)
            DefaultJobTimeout(TimeSpan.Parse(config.DefaultJobTimeout, CultureInfo.InvariantCulture));
        if (config.DefaultMaxRetryCount.HasValue)
            DefaultMaxRetryCount(config.DefaultMaxRetryCount.Value);
        if (config.MaxMessageByteSize.HasValue)
            MaxMessageByteSize(config.MaxMessageByteSize.Value);
        if (config.IanaTimeZoneId != null)
            IanaTimeZoneId(config.IanaTimeZoneId);
        if (config.DataRetentionTtl != null)
            DataRetentionTtl(TimeSpan.Parse(config.DataRetentionTtl, CultureInfo.InvariantCulture));

        foreach (var raw in config.DisabledPriorities ?? [])
        {
            if (!Enum.TryParse(raw, ignoreCase: true, out JobMasterPriority disabledPriority))
                throw new ArgumentException($"Invalid JobMasterPriority value in DisabledPriorities: '{raw}'.");
            DisablePriority(disabledPriority);
        }

        if (config.Standalone)
        {
            // IClusterStandaloneConfigSelector.AddWorker only accepts a name and a batch size — there is
            // no standalone-side equivalent of WorkerLane, ParallelismFactor, SkipWarmUpTime, WorkerMode,
            // BucketQtyConfig, BucketBufferSize, or the cluster-level ConnectionOptions dictionary, so those
            // WorkerJsonConfig/ClusterJsonConfig fields are intentionally ignored here (not applicable to
            // standalone mode), same as when configuring a standalone cluster through the fluent API.
            var standalone = UseStandaloneCluster();
            if (config.RepoType != null) standalone.ClusterRepoType(config.RepoType);
            if (config.ConnectionString != null) standalone.ClusterConnString(config.ConnectionString);
            foreach (var w in config.Workers ?? [])
            {
                if (w.TransferBatchSize.HasValue)
                    standalone.AddWorker(w.WorkerName, w.TransferBatchSize.Value);
                else
                    standalone.AddWorker(w.WorkerName);
            }
        }
        else
        {
            if (config.RepoType != null) ClusterRepoType(config.RepoType);
            if (config.ConnectionString != null) ClusterConnString(config.ConnectionString);
            if (config.ConnectionOptions != null && config.RepoType != null)
                ConnectionOptionsStrategyFactory.Create(config.RepoType).SetOptions(this, ToIgnoreCase(config.ConnectionOptions));

            foreach (var agent in config.AgentConnections ?? [])
            {
                var agentSel = AddAgentConnectionConfig(agent.Name!, agent.RepositoryType, agent.ConnectionString);
                if (agent.ProtectConnectionChanges.HasValue)
                    agentSel.ProtectConnectionChanges(agent.ProtectConnectionChanges.Value);
                if (agent.ConnectionOptions != null && agent.RepositoryType != null)
                    ConnectionOptionsStrategyFactory.Create(agent.RepositoryType).SetOptions(agentSel, ToIgnoreCase(agent.ConnectionOptions));
            }

            foreach (var w in config.Workers ?? [])
            {
                var workerSel = AddWorker(
                    w.WorkerName,
                    w.AgentConnectionName,
                    w.TransferBatchSize ?? JobMasterDefaults.Worker.TransferBatchSize,
                    w.BucketBufferSize ?? JobMasterDefaults.Worker.BucketBufferSize);

                if (w.WorkerLane != null) workerSel.WorkerLane(w.WorkerLane);
                if (w.ParallelismFactor.HasValue) workerSel.ParallelismFactor(w.ParallelismFactor.Value);
                if (w.SkipWarmUpTime == true) workerSel.SkipWarmUpTime();

                if (w.WorkerMode != null)
                {
                    if (!Enum.TryParse(w.WorkerMode, ignoreCase: true, out AgentWorkerMode workerMode))
                        throw new ArgumentException($"Invalid AgentWorkerMode value: '{w.WorkerMode}'.");
                    workerSel.SetWorkerMode(workerMode);
                }

                foreach (var kvp in w.BucketQtyConfig ?? new Dictionary<string, int>())
                {
                    if (Enum.TryParse(kvp.Key, ignoreCase: true, out JobMasterPriority priority))
                        workerSel.BucketQtyConfig(priority, kvp.Value);
                }
            }
        }

        return this;
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

    public IClusterStandaloneConfigSelector ClusterDataRetentionTtl(TimeSpan dataRetentionTtl)
    {
        clusterDefinition.SetDataRetentionTtl(dataRetentionTtl);
        return this;
    }

    public IClusterStandaloneConfigSelector RetainDataForever()
    {
        clusterDefinition.DataRetentionTtl = TimeSpan.Zero;
        return this;
    }

    public IClusterStandaloneConfigSelector DisablePriority(JobMasterPriority priority)
    {
        clusterDefinition.DisablePriority(priority);
        return this;
    }

    public IClusterStandaloneConfigSelector AddWorker(
        string? workerName = null, 
        int batchSize = 250)
    {
        var def = new WorkerDefinition
        {
            WorkerName = workerName ?? string.Empty,
            TransferBatchSize = batchSize,
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
