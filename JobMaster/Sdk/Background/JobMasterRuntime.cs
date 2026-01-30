using System.Collections.Concurrent;
using System.Reflection;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
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

    public IReadOnlyList<IJobMasterBackgroundAgentWorker> GetAllWorkers() => new List<IJobMasterBackgroundAgentWorker>(Workers);

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

        // Default cluster handling
        if (JobMasterClusterConnectionConfig.Default == null)
        {
            if (JobMasterClusterConnectionConfig.ClusterCount == 1)
            {
                JobMasterClusterConnectionConfig.SetDefaultConfig(JobMasterClusterConnectionConfig.GetAllConfigs().First().ClusterId);
            }
            else if (JobMasterClusterConnectionConfig.ClusterCount > 1)
            {
                throw new InvalidOperationException("Multiple clusters configured but no default cluster defined. Mark one as default.");
            }
        }

        foreach (var clusterCnnCfg in JobMasterClusterConnectionConfig.GetAllConfigs())
        {
            var componentFactory = JobMasterClusterAwareComponentFactories.GetFactory(clusterCnnCfg.ClusterId);
            var clusterDefinition = BootstrapBlueprintDefinitions.Clusters.FirstOrDefault(c => c.ClusterId == clusterCnnCfg.ClusterId);
            if (clusterDefinition == null)
            {
                throw new InvalidOperationException("Cluster definition not found");
            }
            var agentDefinitions = clusterDefinition.AgentConnections;
            var workerDefinitions = clusterDefinition.Workers;
            
            var masterConfigService = componentFactory.GetComponent<IMasterClusterConfigurationService>();
          
            var modelToSave = masterConfigService.GetNoAche() ?? new ClusterConfigurationModel(clusterCnnCfg.ClusterId);
            modelToSave.DefaultJobTimeout = clusterDefinition.DefaultJobTimeout ?? modelToSave.DefaultJobTimeout;
            modelToSave.DefaultMaxOfRetryCount = clusterDefinition.DefaultMaxRetryCount ?? modelToSave.DefaultMaxOfRetryCount;
            modelToSave.IanaTimeZoneId = clusterDefinition.IanaTimeZoneId ?? modelToSave.IanaTimeZoneId;
            modelToSave.MaxMessageByteSize = clusterDefinition.MaxMessageByteSize ?? modelToSave.MaxMessageByteSize;
            modelToSave.AdditionalConfig = clusterDefinition.AdditionalConfig ?? modelToSave.AdditionalConfig;
            modelToSave.TransientThreshold = clusterDefinition.TransientThreshold ?? modelToSave.TransientThreshold;
            modelToSave.ClusterMode = clusterDefinition.ClusterMode ?? modelToSave.ClusterMode;

            if (clusterCnnCfg.MirrorLog == JsonlFileLogger.LogMirror)
            {
                JsonlFileLogger.AddLogger(clusterCnnCfg.ClusterId, clusterDefinition.MirrorLogFilePath!, clusterDefinition.MirrorLogMaxBufferItems ?? 500, clusterDefinition.MirrorLogFlushInterval);
            }

            if (clusterDefinition.IanaTimeZoneId != null && modelToSave.IanaTimeZoneId != TimeZoneUtils.GetLocalIanaTimeZoneId())
            {
                throw new InvalidOperationException(
                    "if you want to use agents in different regions please explicitly set the IanaTimeZoneId for the cluster. " +
                    "The cluster IanaTimeZoneId does not match the local timezone" + 
                    " ClusterId: " + clusterDefinition.ClusterId + ", Cluster IanaTimeZoneId: " + clusterDefinition.IanaTimeZoneId + ", Local IanaTimeZoneId: " + modelToSave.IanaTimeZoneId);
            }

            if ((modelToSave.ClusterMode == ClusterMode.Passive || modelToSave.ClusterMode == ClusterMode.Archived) && 
                workerDefinitions.Any(x => x.Mode != AgentWorkerMode.Drain)) 
            {
                throw new InvalidOperationException("Passive and Archived clusters can only have Drain workers");
            }
            
            masterConfigService.Save(modelToSave);
            
            // Ensure no duplicates
            if (agentDefinitions.GroupBy(x => x.AgentConnectionName).Any(x => x.ToList().Count > 1))
            {
                throw new InvalidOperationException("Duplicate agent connection names found");
            }
            
            foreach (var workerDefinition in workerDefinitions)
            {
                if (string.IsNullOrEmpty(workerDefinition.WorkerName))
                {
                    var workerName = JobMasterStringUtils.SanitizeForSegment(Environment.MachineName, 40) + "." + JobMasterIdUtil.NewNanoId();
                    workerDefinition.WorkerName = workerName;
                }
            }
            
            // Ensure no duplicates
            if (workerDefinitions.GroupBy(x => x.WorkerName).Any(x => x.ToList().Count > 1))
            {
                throw new InvalidOperationException("Duplicate worker names found");
            }
            
            foreach (var workerDefinition in workerDefinitions)
            {
                var worker = await JobMasterBackgroundAgentWorker.CreateAsync(
                    serviceProvider,
                    clusterCnnCfg.ClusterId,
                    workerDefinition.AgentConnectionName,
                    workerDefinition.WorkerName,
                    workerDefinition.WorkerLane,
                    workerDefinition.Mode);

                Workers.Add(worker);
            }
        }

        foreach (var def in JobMasterClusterConnectionConfig.GetAllConfigs())
        {
            def.Activate();
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
    
    private IDictionary<string, OperationThrottler> OperationThrottlerPerCluster { get; } = new ConcurrentDictionary<string, OperationThrottler>();
    private OperationThrottler? notStartedClusterOperationThrottler;
    public OperationThrottler GetOperationThrottlerForCluster(string clusterId)
    {
        if (!Started)
        {
            var logger = JobMasterClusterAwareComponentFactories.GetFactory(clusterId).GetComponent<IJobMasterLogger>();
            if (notStartedClusterOperationThrottler == null)
            {
                notStartedClusterOperationThrottler = new OperationThrottler(50, logger);
            }
            
            return notStartedClusterOperationThrottler;
        }
        
        if (!OperationThrottlerPerCluster.TryGetValue(clusterId, out var throttler))
        {
            var logger = JobMasterClusterAwareComponentFactories.GetFactory(clusterId).GetComponent<IJobMasterLogger>();
            var jobMasterClusterConnectionConfig = JobMasterClusterConnectionConfig.Get(clusterId, includeInactive: true);
            throttler = new OperationThrottler(jobMasterClusterConnectionConfig.RuntimeDbOperationThrottleLimit, logger);
            OperationThrottlerPerCluster[clusterId] = throttler;
        }
        
        return throttler;
    }
    

    private IDictionary<string, OperationThrottler> OperationThrottlerPerAgent { get; } = new ConcurrentDictionary<string, OperationThrottler>();
    private OperationThrottler? notStartedAgentOperationThrottler;
    public OperationThrottler GetOperationThrottlerForAgent(string clusterId, string agentConnectionIdOrName)
    {
        if (!Started)
        {
            if (notStartedAgentOperationThrottler == null)
            {
                var logger = JobMasterClusterAwareComponentFactories.GetFactory(clusterId).GetComponent<IJobMasterLogger>();
                notStartedAgentOperationThrottler = new OperationThrottler(50, logger);
            }
            
            return notStartedAgentOperationThrottler;
        }
        
        var key = clusterId + "||" + agentConnectionIdOrName;
        if (!OperationThrottlerPerAgent.TryGetValue(key, out var throttler))
        {
            var logger = JobMasterClusterAwareComponentFactories.GetFactory(clusterId).GetComponent<IJobMasterLogger>();
            var runtimeDbOperationThrottleLimit = JobMasterClusterConnectionConfig
                .Get(clusterId, includeInactive: true)
                .GetAgentConnectionConfig(agentConnectionIdOrName).RuntimeDbOperationThrottleLimit;
            throttler = new OperationThrottler(runtimeDbOperationThrottleLimit, logger);
            OperationThrottlerPerAgent[key] = throttler;
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
                try { return a.GetTypes(); }
                catch { return Array.Empty<Type>(); } // skip dynamic/unloaded
            })
            .Where(t => !t.IsAbstract && t.GetInterfaces().Any(i => i.Name == nameof(IStaticRecurringSchedulesProfile)));

        IList<(StaticRecurringSchedulesProfileInfo info, RecurringScheduleDefinitionCollection collection)> profileInfos = 
            new List<(StaticRecurringSchedulesProfileInfo info, RecurringScheduleDefinitionCollection collection)>();
        
        
        foreach (var pt in profileTypes)
        {
            // 2) Read static members
            var profileId = (string?)pt.GetProperty("ProfileId", BindingFlags.Public | BindingFlags.Static)?.GetValue(null)
                            ?? throw new InvalidOperationException($"ProfileId missing on {pt.FullName}");
            var profileCluster = (string?)pt.GetProperty("ClusterId", BindingFlags.Public | BindingFlags.Static)?.GetValue(null);
            var effectiveCluster = string.IsNullOrWhiteSpace(profileCluster) ? defaultClusterId : profileCluster;
            
            var workerLane = (string?)pt.GetProperty("WorkerLane", BindingFlags.Public | BindingFlags.Static)?.GetValue(null);

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
        

        // // 5) Upsert each desired (one-by-one)
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
