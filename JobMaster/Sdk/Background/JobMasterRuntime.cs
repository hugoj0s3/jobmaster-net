using System.Collections.Concurrent;
using System.Reflection;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Sdk.Background;

internal class JobMasterRuntime : IJobMasterRuntime
{
    private readonly List<JobMasterBackgroundAgentWorker> Workers = new List<JobMasterBackgroundAgentWorker>();

    public bool Started { get; private set; } = false;
    public DateTime? StartedAt { get; private set; }

    public DateTime? StartingAt { get; private set; }

    public IReadOnlyList<IJobMasterBackgroundAgentWorker> GetAllWorkers() =>
        new List<IJobMasterBackgroundAgentWorker>(Workers);

    public bool IsOnWarmUpTime()
    {
        return Started &&
               StartedAt.HasValue &&
               (DateTime.UtcNow - StartedAt.Value) <= TimeSpan.FromMinutes(2.5);
    }

    public JobMasterRuntime()
    {
    }

    public async Task StartAsync(IServiceProvider serviceProvider)
    {
        this.StartingAt = DateTime.UtcNow;
        if (serviceProvider == null) throw new ArgumentNullException(nameof(serviceProvider));

        if (Started)
        {
            throw new InvalidOperationException("JobMasterRuntime is already started");
        }

        using var scope = serviceProvider.CreateScope();

        var runtimeSetups = scope.ServiceProvider.GetServices<IJobMasterRuntimeSetup>().ToList();
        foreach (var runtimeSetup in runtimeSetups)
        {
            await runtimeSetup.ValidateAsync(scope.ServiceProvider);
        }

        foreach (var runtimeSetup in runtimeSetups)
        {
            await runtimeSetup.OnStartingAsync(scope.ServiceProvider);
        }

        PreValidation();

        foreach (var clusterCnnCfg in JobMasterClusterConnectionConfig.GetAllConfigs())
        {
            var componentFactory = JobMasterClusterAwareComponentFactories.GetFactory(clusterCnnCfg.ClusterId);
            var agentComponentFactory = componentFactory.GetComponent<IAgentComponentFactory>();
            var masterConfigService = componentFactory.GetComponent<IMasterClusterConfigurationService>();
            var masterAgentConnectionService = componentFactory.GetComponent<IMasterAgentConnectionService>();
            var logger = componentFactory.GetComponent<IJobMasterLogger>();

            var clusterDefinition = BootstrapBlueprintDefinitions.Clusters.Single(c =>
                string.Equals(c.ClusterId, clusterCnnCfg.ClusterId, StringComparison.OrdinalIgnoreCase));

            var agentDefinitions = clusterDefinition.AgentConnections;
            foreach (var agentDefinition in agentDefinitions)
            {
                var agentConfig = clusterCnnCfg.TryGetAgentConnectionConfig(agentDefinition.AgentConnectionName);
                if (agentConfig == null)
                {
                    throw new Exception($"Agent connection {agentDefinition.AgentConnectionName} not found");
                }

                var agentConnectionId = new AgentConnectionId(agentConfig.Id);
                var existingConnection =
                    await masterAgentConnectionService.GetConnectionAsync(agentConnectionId, useCache: false);

                var fingerprintResolver = agentComponentFactory.GetFingerprintResolver(agentConfig.Id);
                var fingerprint =
                    await fingerprintResolver.GiveYourFingerprintAsync(agentDefinition.ClusterId, agentConfig.Id);

                if (existingConnection != null && existingConnection.Fingerprint != fingerprint)
                {
                    if (agentDefinition.ProtectConnectionChanges)
                    {
                        throw new Exception(
                            $"Agent connection {agentDefinition.AgentConnectionName} fingerprint has changed, " +
                            $"please ensure the connection {agentDefinition.AgentConnectionName} is not modified.");
                    }

                    logger.Warn(
                        $"Agent connection {agentDefinition.AgentConnectionName} fingerprint has changed, updating...");
                }

                await masterAgentConnectionService.SaveConnectionAsync(agentConnectionId, agentConfig.RepositoryTypeId,
                    fingerprint, agentDefinition.ProtectConnectionChanges);
            }

            if (clusterDefinition.IsStandalone)
            {
                var standaloneConfig = clusterCnnCfg.TryGetAgentConnectionConfig(JobMasterConstants.StandaloneAgentConnName);
                if (standaloneConfig != null)
                {
                    var standaloneId = new AgentConnectionId(standaloneConfig.Id);
                    var fingerprintResolver = agentComponentFactory.GetFingerprintResolver(standaloneConfig.Id);
                    var fingerprint = await fingerprintResolver.GiveYourFingerprintAsync(clusterDefinition.ClusterId!, standaloneConfig.Id);
                    await masterAgentConnectionService.SaveConnectionAsync(standaloneId, standaloneConfig.RepositoryTypeId, fingerprint, protectChanges: false);
                }
            }

            if (clusterDefinition.Workers.Any(w => w.Mode.IsFullOr(AgentWorkerMode.Coordinator)))
            {
                var fallbackConfig = clusterCnnCfg.GetAgentConnectionConfig(JobMasterConstants.MasterFallbackAgentConnName);
                var fallbackId = new AgentConnectionId(fallbackConfig.Id);
                var fingerprintResolver = agentComponentFactory.GetFingerprintResolver(fallbackConfig.Id);
                var fingerprint = await fingerprintResolver.GiveYourFingerprintAsync(clusterDefinition.ClusterId!, fallbackConfig.Id);
                await masterAgentConnectionService.SaveConnectionAsync(fallbackId, fallbackConfig.RepositoryTypeId, fingerprint, protectChanges: false);
            }

            var workerDefinitions = clusterDefinition.Workers;

            var modelToSave = masterConfigService.GetFresh() ?? new ClusterConfigurationModel(clusterCnnCfg.ClusterId);
            modelToSave.DefaultJobTimeout = clusterDefinition.DefaultJobTimeout ?? modelToSave.DefaultJobTimeout;
            modelToSave.DefaultMaxOfRetryCount =
                clusterDefinition.DefaultMaxRetryCount ?? modelToSave.DefaultMaxOfRetryCount;
            modelToSave.IanaTimeZoneId = clusterDefinition.IanaTimeZoneId ?? modelToSave.IanaTimeZoneId;
            modelToSave.MaxMessageByteSize = clusterDefinition.MaxMessageByteSize ?? modelToSave.MaxMessageByteSize;
            modelToSave.AdditionalConfig = clusterDefinition.AdditionalConfig ?? modelToSave.AdditionalConfig;
            modelToSave.TransientThreshold = clusterDefinition.TransientThreshold ?? modelToSave.TransientThreshold;
            modelToSave.DataRetentionTtl = clusterDefinition.DataRetentionTtl ?? modelToSave.DataRetentionTtl;
            modelToSave.ClusterMode = clusterDefinition.ClusterMode ?? modelToSave.ClusterMode;
            modelToSave.IsStandalone = clusterDefinition.IsStandalone;

            if (clusterCnnCfg.MirrorLog == JsonlFileLogger.LogMirror)
            {
                JsonlFileLogger.AddLogger(clusterCnnCfg.ClusterId, clusterDefinition.MirrorLogFilePath!,
                    clusterDefinition.MirrorLogMaxBufferItems ?? 500, clusterDefinition.MirrorLogFlushInterval);
            }

            masterConfigService.Save(modelToSave);

            if (!clusterDefinition.IsStandalone)
            {
                var bucketService = componentFactory.GetComponent<IMasterBucketsService>();
                var existingBuckets = await bucketService.QueryAllNoCacheAsync();

                // Create a drainer for standalone buckets. It can happen when a standalone cluster transitions to a non-standalone cluster.
                // workerDefinitions.Any() is to not add in publisher only app instance.
                if (existingBuckets.Any(x => x.IsStandaloneBucket(clusterDefinition.ClusterId!)) &&
                    workerDefinitions.Any())
                {
                    var lanes = existingBuckets.Where(x => x.IsStandaloneBucket(clusterDefinition.ClusterId!))
                        .Select(x => x.WorkerLane)
                        .Distinct()
                        .ToList();

                    foreach (var lane in lanes)
                    {
                        var workerDefinition = new WorkerDefinition()
                        {
                            AgentConnectionName = JobMasterConstants.StandaloneAgentConnName,
                            WorkerName = "StandaloneDrainer",
                            WorkerLane = lane,
                            Mode = AgentWorkerMode.Drain,
                            ClusterId = clusterDefinition.ClusterId!,
                            TransferBatchSize = 1000,
                        };

                        var worker = await JobMasterBackgroundAgentWorker.CreateAsync(
                            serviceProvider,
                            workerDefinition);

                        Workers.Add(worker);
                    }
                }
            }

            foreach (var workerDefinition in workerDefinitions)
            {
                var worker = await JobMasterBackgroundAgentWorker.CreateAsync(
                    serviceProvider,
                    workerDefinition);

                Workers.Add(worker);
            }
        }

        foreach (var def in JobMasterClusterConnectionConfig.GetAllConfigs())
        {
            def.MarkAsReady();
        }

        foreach (var worker in Workers)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(JobMasterRandomUtil.GetInt(100, 500)));

            await worker.StartAsync();
        }

        BootstrapStaticRecurringSchedules(JobMasterClusterConnectionConfig.Default!.ClusterId);

        Started = true;
        StartedAt = DateTime.UtcNow;

        BootstrapBlueprintDefinitions.Clear();

        await Task.Delay(TimeSpan.FromSeconds(1));
    }

    private static void PreValidation()
    {
        // Default cluster handling
        if (JobMasterClusterConnectionConfig.Default == null)
        {
            if (JobMasterClusterConnectionConfig.ClusterCount == 1)
            {
                JobMasterClusterConnectionConfig.SetDefaultConfig(JobMasterClusterConnectionConfig.GetAllConfigs()
                    .First().ClusterId);
            }
            else if (JobMasterClusterConnectionConfig.ClusterCount > 1)
            {
                throw new InvalidOperationException(
                    "Multiple clusters configured but no default cluster defined. Mark one as default.");
            }
        }

        var clusterDupes = JobMasterClusterConnectionConfig
            .GetAllConfigs()
            .GroupBy(x => JobMasterStringUtils.NormalizeId(x.ClusterId)).Where(x => x.Count() > 1)
            .ToList();

        if (clusterDupes.Any())
        {
            throw new InvalidOperationException(
                $"Multiple clusters configured with the same id. {string.Join(", ", clusterDupes.Select(x => x.Key))}");
        }

        // Validation of all clusters first.
        foreach (var clusterCnnCfg in JobMasterClusterConnectionConfig.GetAllConfigs())
        {
            var componentFactory = JobMasterClusterAwareComponentFactories.GetFactory(clusterCnnCfg.ClusterId);
            var clusterDefinition = BootstrapBlueprintDefinitions.Clusters.SingleOrDefault(c =>
                string.Equals(c.ClusterId, clusterCnnCfg.ClusterId, StringComparison.OrdinalIgnoreCase));

            if (clusterDefinition == null)
            {
                throw new InvalidOperationException("Cluster definition not found");
            }

            if (string.IsNullOrWhiteSpace(clusterDefinition.ConnString) ||
                string.IsNullOrWhiteSpace(clusterDefinition.RepoType))
            {
                throw new InvalidOperationException(
                    "Cluster definition is missing connection string or repository type");
            }

            var agentDefinitions = clusterDefinition.AgentConnections;

            var missingCnnStringOrRepoType = agentDefinitions.Where(x =>
                string.IsNullOrWhiteSpace(x.AgentConnString) || string.IsNullOrWhiteSpace(x.AgentRepoType)).ToList();
            if (missingCnnStringOrRepoType.Any())
            {
                var agentNames = string.Join(", ", missingCnnStringOrRepoType.Select(x => x.AgentConnectionName));
                throw new InvalidOperationException(
                    @$"Agent connection is missing connection string or repository type. Connection: {agentNames}");
            }

            var workerDefinitions = clusterDefinition.Workers;

            var masterConfigService = componentFactory.GetComponent<IMasterClusterConfigurationService>();

            var existingClusterConfig = masterConfigService.GetFresh();
            var existingTimezoneId = existingClusterConfig?.IanaTimeZoneId ?? TimeZoneUtils.GetLocalIanaTimeZoneId();

            if (clusterDefinition.IanaTimeZoneId != null &&
                existingTimezoneId != TimeZoneUtils.GetLocalIanaTimeZoneId())
            {
                throw new InvalidOperationException(
                    "if you want to use agents in different regions please explicitly set the IanaTimeZoneId for the cluster. " +
                    "The cluster IanaTimeZoneId does not match the local timezone" +
                    " ClusterId: " + clusterDefinition.ClusterId + ", Defined IanaTimeZoneId: " +
                    clusterDefinition.IanaTimeZoneId + ", Existing IanaTimeZoneId Configured: " + existingTimezoneId);
            }

            // Ensure no duplicates
            var agentDupes = agentDefinitions
                .GroupBy(x => JobMasterStringUtils.NormalizeId(x.AgentConnectionName))
                .Where(x => x.ToList().Count > 1).ToList();
            if (agentDupes.Any())
            {
                throw new InvalidOperationException(
                    $"Duplicate agent connection names found. {string.Join(", ", agentDupes.Select(x => x.Key))}");
            }

            if ((clusterDefinition.ClusterMode == ClusterMode.Passive ||
                 clusterDefinition.ClusterMode == ClusterMode.Archived) &&
                workerDefinitions.Any(x => x.BucketQty.Any(y => y.Value >= 1)))
            {
                throw new InvalidOperationException("Passive and Archived clusters can not have buckets defined");
            }

            if (agentDefinitions.Any(x => string.Equals(
                    x.AgentConnectionName,
                    JobMasterConstants.StandaloneAgentConnName,
                    StringComparison.OrdinalIgnoreCase)))
            {
                throw new InvalidOperationException(
                    $"{JobMasterConstants.StandaloneAgentConnName} is reserved for standalone agents. Cannot be used for other agents.");
            }

            if (agentDefinitions.Any(x => string.Equals(
                    x.AgentConnectionName,
                    JobMasterConstants.MasterFallbackAgentConnName,
                    StringComparison.OrdinalIgnoreCase)))
            {
                throw new InvalidOperationException(
                    $"{JobMasterConstants.MasterFallbackAgentConnName} is reserved for fallback buckets. Cannot be used for other agents.");
            }

            if (workerDefinitions.Any(x => string.Equals(
                    x.AgentConnectionName,
                    JobMasterConstants.MasterFallbackAgentConnName,
                    StringComparison.OrdinalIgnoreCase)))
            {
                throw new InvalidOperationException(
                    $"{JobMasterConstants.MasterFallbackAgentConnName} is reserved for fallback buckets. Cannot be used as a worker's AgentConnectionName.");
            }

            if (!clusterDefinition.IsStandalone && workerDefinitions.Any(x => string.Equals(
                    x.AgentConnectionName,
                    JobMasterConstants.StandaloneAgentConnName,
                    StringComparison.OrdinalIgnoreCase)))
            {
                throw new InvalidOperationException(
                    $"{JobMasterConstants.StandaloneAgentConnName} is reserved for standalone clusters. Cannot be used as a worker's AgentConnectionName in a non-standalone cluster.");
            }

            if (workerDefinitions.Any(x => x.Mode == AgentWorkerMode.Coordinator && !string.IsNullOrEmpty(x.AgentConnectionName)))
            {
                throw new InvalidOperationException(
                    "Coordinator workers must not have an AgentConnectionName configured.");
            }

            if (!clusterDefinition.IsStandalone)
            {
                // Standalone workers always get AgentConnectionName forced to StandaloneAgentConnName by the
                // builder, so there's nothing to cross-reference there — only check explicit clusters.
                var invalidWorkers = workerDefinitions
                    .Where(x => x.Mode != AgentWorkerMode.Coordinator)
                    .Where(x => !agentDefinitions.Any(a => string.Equals(a.AgentConnectionName, x.AgentConnectionName, StringComparison.OrdinalIgnoreCase)))
                    .Select(x => string.IsNullOrEmpty(x.WorkerName) ? "(unnamed)" : x.WorkerName)
                    .ToList();

                if (invalidWorkers.Any())
                {
                    throw new InvalidOperationException(
                        $"The following workers have an AgentConnectionName that does not match any registered agent connection: {string.Join(", ", invalidWorkers)}");
                }
            }

            if (clusterDefinition.IsStandalone && agentDefinitions.Any())
            {
                throw new InvalidOperationException(
                    "Standalone clusters cannot have agents defined. The standalone stays in the master db together with the cluster");
            }


            // Ensure no duplicates
            var workDupes = workerDefinitions
                .Where(x => !string.IsNullOrEmpty(x.WorkerName))
                .GroupBy(x => JobMasterStringUtils.NormalizeId(x.WorkerName!)).Where(x => x.ToList().Count > 1)
                .ToList();
            if (workDupes.Any())
            {
                throw new InvalidOperationException(
                    $"Duplicate worker names found {string.Join(", ", workDupes.Select(x => x.Key))}");
            }

            var distinctWorkLanes = workerDefinitions
                .Where(x => !string.IsNullOrEmpty(x.WorkerLane))
                .Select(x => x.WorkerLane)
                .Distinct()
                .ToList();

            var workLanesDupes = distinctWorkLanes.GroupBy(x => JobMasterStringUtils.NormalizeId(x!))
                .Where(x => x.ToList().Count > 1).ToList();
            if (workLanesDupes.Any())
            {
                throw new InvalidOperationException(
                    $"Duplicate worker lanes found {string.Join(", ", workLanesDupes.Select(x => x.Key))}");
            }

            if (clusterDefinition.DisabledPriorities.Any())
            {
                var handlerTypes = AppDomain.CurrentDomain.GetAssemblies()
                    .SelectMany(a =>
                    {
                        try { return a.GetTypes(); }
                        catch { return Array.Empty<Type>(); }
                    })
                    .Where(t => !t.IsAbstract && !t.IsInterface && typeof(IJobMasterHandler).IsAssignableFrom(t));

                foreach (var handlerType in handlerTypes)
                {
                    var p = handlerType.GetCustomAttributes(false)
                        .OfType<JobMasterPriorityAttribute>()
                        .FirstOrDefault()?.Priority ?? JobMasterPriority.Medium;

                    if (clusterDefinition.DisabledPriorities.Contains(p))
                    {
                        throw new InvalidOperationException(
                            $"Handler '{handlerType.FullName}' uses priority {p}, which is disabled " +
                            $"on cluster '{clusterDefinition.ClusterId}'. " +
                            $"Remove or change the [JobMasterPriority] attribute on that handler.");
                    }
                }
            }
        }
    }

    private IDictionary<string, OperationThrottler> OperationLimiterPerCluster { get; } =
        new ConcurrentDictionary<string, OperationThrottler>();

    private OperationThrottler? notStartedClusterOperationThrottler;

    public OperationThrottler GetOperationLimiterForCluster(string clusterId)
    {
        if (!Started)
        {
            if (notStartedClusterOperationThrottler == null)
            {
                notStartedClusterOperationThrottler = new OperationThrottler(50);
            }

            return notStartedClusterOperationThrottler;
        }

        if (!OperationLimiterPerCluster.TryGetValue(clusterId, out var throttler))
        {
            var jobMasterClusterConnectionConfig =
                JobMasterClusterConnectionConfig.Get(clusterId, includeNotReady: true);
            throttler = new OperationThrottler(jobMasterClusterConnectionConfig.RuntimeDbOperationLimit);
            OperationLimiterPerCluster[clusterId] = throttler;
        }

        return throttler;
    }


    private IDictionary<string, OperationThrottler> OperationLimiterPerAgent { get; } =
        new ConcurrentDictionary<string, OperationThrottler>();

    private OperationThrottler? notStartedAgentOperationLimiter;

    public OperationThrottler GetOperationLimiterForAgent(string clusterId, string agentConnectionIdOrName)
    {
        if (!Started)
        {
            if (notStartedAgentOperationLimiter == null)
            {
                notStartedAgentOperationLimiter = new OperationThrottler(50);
            }

            return notStartedAgentOperationLimiter;
        }

        var key = clusterId + "||" + agentConnectionIdOrName;
        if (!OperationLimiterPerAgent.TryGetValue(key, out var throttler))
        {
            var runtimeDbOperationLimit = JobMasterClusterConnectionConfig
                .Get(clusterId, includeNotReady: true)
                .GetAgentConnectionConfig(agentConnectionIdOrName).RuntimeDbOperationLimit;
            throttler = new OperationThrottler(runtimeDbOperationLimit);
            OperationLimiterPerAgent[key] = throttler;
        }

        return throttler;
    }

    public int CountWorkersForCluster(string clusterId)
    {
        if (string.IsNullOrEmpty(clusterId))
        {
            return 0;
        }

        return Workers.Count(w => w.ClusterConnConfig.ClusterId == clusterId);
    }

    public void Stop()
    {
        List<JobMasterBackgroundAgentWorker> toStop;
        toStop = new List<JobMasterBackgroundAgentWorker>(Workers);
        Workers.Clear();

        foreach (var w in toStop)
        {
            try
            {
                w.RequestStop();
                w.Dispose();
            }
            catch
            {
                // Swallow to attempt stopping others; consider logging hook later
            }
        }
    }

    private void BootstrapStaticRecurringSchedules(string defaultClusterId)
    {
        // 1) Discover profile types (static interface)
        var profileTypes = AppDomain.CurrentDomain.GetAssemblies()
            .SelectMany(a =>
            {
                try
                {
                    return a.GetTypes();
                }
                catch
                {
                    return Array.Empty<Type>();
                } // skip dynamic/unloaded
            })
            .Where(t => !t.IsAbstract &&
                        t.GetInterfaces().Any(i => i.Name == nameof(IStaticRecurringSchedulesProfile)));

        IList<(StaticRecurringSchedulesProfileInfo info, RecurringScheduleDefinitionCollection collection)>
            profileInfos =
                new List<(StaticRecurringSchedulesProfileInfo info, RecurringScheduleDefinitionCollection collection
                    )>();


        foreach (var pt in profileTypes)
        {
            // 2) Read static members
            var profileId =
                (string?)pt.GetProperty("ProfileId", BindingFlags.Public | BindingFlags.Static)?.GetValue(null)
                ?? throw new InvalidOperationException($"ProfileId missing on {pt.FullName}");
            var profileCluster = (string?)pt.GetProperty("ClusterId", BindingFlags.Public | BindingFlags.Static)
                ?.GetValue(null);
            var effectiveCluster = string.IsNullOrWhiteSpace(profileCluster) ? defaultClusterId : profileCluster;

            var workerLane = (string?)pt.GetProperty("WorkerLane", BindingFlags.Public | BindingFlags.Static)
                ?.GetValue(null);

            // Validate profile info
            var info = new StaticRecurringSchedulesProfileInfo(profileId, effectiveCluster!, workerLane);
            if (!info.IsValid)
                throw new InvalidOperationException($"Invalid ProfileId/ClusterId on profile {pt.FullName}");

            // 3) Build collection and invoke static Config
            var collection = new RecurringScheduleDefinitionCollection(info, effectiveCluster!);

            var configMethod = pt.GetMethod("Config", BindingFlags.Public | BindingFlags.Static);
            if (configMethod == null)
                throw new InvalidOperationException($"Config(...) not found on profile {pt.FullName}");

            configMethod.Invoke(null, new object[] { collection });

            profileInfos.Add((info, collection));
        }


        // Validate schedule priorities against disabled priorities before upserting
        foreach (var cfg in profileInfos)
        {
            var clusterDef = BootstrapBlueprintDefinitions.Clusters.SingleOrDefault(c =>
                string.Equals(c.ClusterId, cfg.info.ClusterId, StringComparison.OrdinalIgnoreCase));

            if (clusterDef?.DisabledPriorities.Any() != true) continue;

            foreach (var def in cfg.collection.ToReadOnly())
            {
                // null priority defers to the handler attribute — already validated in PreValidation.
                // Only reject an explicitly-set disabled priority here.
                if (def.Priority.HasValue && clusterDef.DisabledPriorities.Contains(def.Priority.Value))
                {
                    throw new InvalidOperationException(
                        $"Static recurring schedule '{def.Id}' uses priority {def.Priority.Value}, " +
                        $"which is disabled on cluster '{cfg.info.ClusterId}'.");
                }
            }
        }

        // Upsert each desired (one-by-one)
        foreach (var cfg in profileInfos)
        {
            var clusterId = cfg.info.ClusterId;
            if (string.IsNullOrEmpty(clusterId))
            {
                clusterId = defaultClusterId;
            }

            var componentFactory = JobMasterClusterAwareComponentFactories.GetFactory(clusterId);
            var masterRecurringService = componentFactory.GetService<IMasterRecurringSchedulesService>();
            var masterDistributedLockerService = componentFactory.GetService<IMasterDistributedLockerService>();
            var jobMasterLockKeys = new JobMasterLockKeys(clusterId);
            foreach (var config in cfg.collection.ToReadOnly())
            {
                var lockKey = jobMasterLockKeys.RecurringScheduleUpsertStatic(config.Id);
                var lockToken = masterDistributedLockerService.TryLock(lockKey, TimeSpan.FromMinutes(1));
                if (lockToken == null)
                {
                    continue;
                }

                try
                {
                    masterRecurringService.UpsertStatic(config);
                    StaticRecurringDefinitionIdsKeeper.Add(clusterId, config.Id);
                }
                finally
                {
                    masterDistributedLockerService.ReleaseLock(lockKey, lockToken);
                }
            }
        }
    }
}
