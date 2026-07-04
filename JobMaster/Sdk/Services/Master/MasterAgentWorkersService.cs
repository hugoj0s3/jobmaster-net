using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.LocalCache;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Cache;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Services.Master;

internal class MasterAgentWorkersService : JobMasterClusterAwareComponent, IMasterAgentWorkersService
{
    private IMasterClusterConfigurationService masterClusterConfigurationService = null!;
    private IMasterChangesSentinelService masterChangesSentinelService = null!;
    private IMasterHeartbeatService masterHeartbeatService = null!;
    private IMasterGenericRecordRepository masterGenericRecordRepository = null!;
    private readonly IMasterHostService masterHostService;

    private IJobMasterInMemoryCache jobMasterInMemoryCache = null!;
    private JobMasterInMemoryKeys cacheKeys = null!;
    private JobMasterSentinelKeys sentinelKeys = null!;
    private HostId? hostId;

    private readonly RetryDeadlockPolicy retryDeadlockPolicy;
    private readonly SentinelCachedReader sentinelCachedReader;

    public MasterAgentWorkersService(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IJobMasterInMemoryCache jobMasterInMemoryCache,
        IMasterClusterConfigurationService masterClusterConfigurationService,
        IMasterChangesSentinelService masterChangesSentinelService,
        IMasterHeartbeatService masterHeartbeatService,
        IMasterGenericRecordRepository masterGenericRecordRepository,
        IKnownExceptionIdentifier knownExceptionIdentifier,
        IMasterHostService masterHostService) : base(clusterConnectionConfig)
    {
        this.jobMasterInMemoryCache = jobMasterInMemoryCache;
        this.masterClusterConfigurationService = masterClusterConfigurationService;
        this.masterChangesSentinelService = masterChangesSentinelService;
        this.masterHeartbeatService = masterHeartbeatService;
        this.masterGenericRecordRepository = masterGenericRecordRepository;
        this.masterHostService = masterHostService;

        cacheKeys = new JobMasterInMemoryKeys(clusterConnectionConfig.ClusterId);
        sentinelKeys = new JobMasterSentinelKeys(clusterConnectionConfig.ClusterId);

        this.retryDeadlockPolicy =
            new RetryDeadlockPolicy(knownExceptionIdentifier, TimeSpan.FromMilliseconds(250), 3);

        this.sentinelCachedReader =
            new SentinelCachedReader(jobMasterInMemoryCache, masterChangesSentinelService);
    }
    
    public IList<AgentWorkerModel> QueryWorkers(string? agentConnectionId = null, bool useCache = true)
    {
        var all = GetAllAgentWorkers(useCache);
        var agentWorkerRecords = all.Where(x => agentConnectionId == null || x.AgentConnectionId == agentConnectionId).ToList();
        return ToModel(agentWorkerRecords);
    }

    public async Task<IList<AgentWorkerModel>> QueryWorkersAsync(string? agentConnectionId = null, bool useCache = true)
    {
        var all = await GetAllAgentWorkersAsync(useCache);
        var agentWorkerRecords = all.Where(x => agentConnectionId == null || x.AgentConnectionId == agentConnectionId).ToList();
        return ToModel(agentWorkerRecords);
    }

    public AgentWorkerModel? GetWorker(string workerId)
    {
        var all = GetAllAgentWorkers(useCache: false);
        var worker = all.FirstOrDefault(x => x.Id == workerId);
        return ToModel(worker);
    }

    public async Task<AgentWorkerModel?> GetWorkerAsync(string workerId)
    {
        var all = await GetAllAgentWorkersAsync();
        var worker = all.FirstOrDefault(x => x.Id == workerId);

        return ToModel(worker);
    }

    public async Task<(string workerId, HostId hostId)> RegisterWorkerAsync(string? agentConnectionId, string? workerName, string? workerLane,
        AgentWorkerMode mode, double parallelismFactor)
    {
        var worker = await CreateValidatedWorkerAsync(agentConnectionId, workerName, workerLane, mode, parallelismFactor);
        var workerRecord = GenericRecordEntry.Create(ClusterConnConfig.ClusterId, MasterGenericRecordGroupIds.AgentWorker, worker.Id, worker);
        NotifyChanges();
        await masterGenericRecordRepository.InsertAsync(workerRecord);
        
        return (worker.Id, HostId.Recover(worker.HostDisplayName, worker.HostId));
    }

    public void DeleteWorker(string workerId)
    {
        var existingWorker = this.GetWorker(workerId);
        if (existingWorker == null) 
        {
            return;
        }
        
        if (existingWorker.IsAlive) 
        {
            throw new InvalidOperationException($"Worker with id {workerId} is in use and cannot be deleted.");
        }
        
        NotifyChanges();
        masterGenericRecordRepository.Delete(MasterGenericRecordGroupIds.AgentWorker, workerId);
    }

    public async Task DeleteWorkerAsync(string workerId)
    {
        var existingWorker = await this.GetWorkerAsync(workerId);
        if (existingWorker == null) 
        {
            return;
        }
        
        if (existingWorker.IsAlive) 
        {
            throw new InvalidOperationException($"Worker with id {workerId} is in use and cannot be deleted.");
        }
        
        NotifyChanges();
        await masterGenericRecordRepository.DeleteAsync(MasterGenericRecordGroupIds.AgentWorker, workerId);
    }

    public async Task StopGracefulWorkerAsync(string workerId, TimeSpan? gracePeriod = null)
    {
        var worker = await this.GetWorkerAsync(workerId);
        if (worker == null)
        {
            throw new InvalidOperationException($"Worker with id {workerId} does not exist.");
        }
        
        worker.StopRequestedAt = DateTime.UtcNow;
        worker.StopGracePeriod = gracePeriod ?? JobMasterConstants.DefaultGracefulStopPeriod;
        var record = new AgentWorkerRecord(ClusterConnConfig.ClusterId)
        {
            AgentConnectionId = worker.AgentConnectionId?.IdValue ?? string.Empty,
            Name = worker.Name,
            Id = worker.Id,
            Mode = worker.Mode,
            WorkerLane = worker.WorkerLane,
            ParallelismFactor = worker.ParallelismFactor,
            StopRequestedAt = worker.StopRequestedAt,
            StopGracePeriod = worker.StopGracePeriod,
        };
       
        NotifyChanges();
        await masterGenericRecordRepository.UpsertAsync(GenericRecordEntry.Create(ClusterConnConfig.ClusterId, MasterGenericRecordGroupIds.AgentWorker, record.Id, record));
    }

    private IList<AgentWorkerRecord> GetAllAgentWorkers(bool useCache = true)
    {
        if (!useCache)
        {
            // Bypass the sentinel check but still warm the cache so subsequent cached
            // reads don't double-fetch.
            var fresh = retryDeadlockPolicy.Exec(FetchAllAgentWorkers);
            jobMasterInMemoryCache.Set(cacheKeys.AllAgentsWorkers(), fresh);
            return fresh;
        }

        return retryDeadlockPolicy.Exec(() => sentinelCachedReader.GetOrFetch(
            cacheKey: cacheKeys.AllAgentsWorkers(),
            sentinelKey: sentinelKeys.AgentsAndWorkers(),
            factory: FetchAllAgentWorkers,
            allowedDiscrepancy: JobMasterConstants.AgentWorkersAllowDiscrepancy,
            valueFactoryLockDuration: TimeSpan.FromSeconds(2),
            durationToExpire: JobMasterConstants.DefaultCacheEntryExpiry));
    }

    private async Task<IList<AgentWorkerRecord>> GetAllAgentWorkersAsync(bool useCache = true)
    {
        if (!useCache)
        {
            // Bypass the sentinel check but still warm the cache so subsequent cached
            // reads don't double-fetch.
            var fresh = await retryDeadlockPolicy.ExecAsync(FetchAllAgentWorkersAsync);
            jobMasterInMemoryCache.Set(cacheKeys.AllAgentsWorkers(), fresh);
            return fresh;
        }

        return await retryDeadlockPolicy.ExecAsync(() => sentinelCachedReader.GetOrFetchAsync(
            cacheKey: cacheKeys.AllAgentsWorkers(),
            sentinelKey: sentinelKeys.AgentsAndWorkers(),
            factory: FetchAllAgentWorkersAsync,
            allowedDiscrepancy: JobMasterConstants.AgentWorkersAllowDiscrepancy,
            valueFactoryLockDuration: TimeSpan.FromSeconds(2),
            durationToExpire: JobMasterConstants.DefaultCacheEntryExpiry));
    }

    private IList<AgentWorkerRecord> FetchAllAgentWorkers()
    {
        var allWorkerRecords = masterGenericRecordRepository.Query(MasterGenericRecordGroupIds.AgentWorker);
        return allWorkerRecords.Select(x => x.ToObject<AgentWorkerRecord>()).ToList();
    }

    private async Task<IList<AgentWorkerRecord>> FetchAllAgentWorkersAsync()
    {
        var allWorkerRecords = await masterGenericRecordRepository.QueryAsync(MasterGenericRecordGroupIds.AgentWorker);
        return allWorkerRecords.Select(x => x.ToObject<AgentWorkerRecord>()).ToList();
    }
    
    private AgentWorkerModel? ToModel(AgentWorkerRecord? worker)
    {
        if (worker == null) return null;

        var heartbeats = masterHeartbeatService.GetLastHeartbeats(
            ResourceHeartbeatType.AgentWorker, new List<string> { worker.Id });
        var lastHeartbeat = heartbeats.GetOrDefault<DateTime?>(worker.Id) ?? worker.CreatedAt;
        var isAlive = DateTime.UtcNow - lastHeartbeat < JobMasterConstants.AgentHeartbeatThreshold;
        return worker.ToModel(lastHeartbeat, isAlive);
    }

    private IList<AgentWorkerModel> ToModel(IList<AgentWorkerRecord> workers)
    {
        var heartbeats = masterHeartbeatService.GetLastHeartbeats(ResourceHeartbeatType.AgentWorker, workers.Select(x => x.Id).ToList());
        var heartbeatThreshold = JobMasterConstants.AgentHeartbeatThreshold;

        return workers.Select(x =>
        {
            var lastHeartbeat = heartbeats.GetOrDefault<DateTime?>(x.Id) ?? x.CreatedAt;
            var isAlive = DateTime.UtcNow - lastHeartbeat < heartbeatThreshold;
            return x.ToModel(lastHeartbeat, isAlive);
        }).ToList();
    }
    
    private async Task<AgentWorkerRecord> CreateValidatedWorkerAsync(
        string? agentConnectionId,
        string? workerName,
        string? workerLane, 
        AgentWorkerMode mode,
        double parallelismFactor)
    {
        if (!string.IsNullOrEmpty(workerLane) && !JobMasterStringUtils.IsValidForSegment(workerLane!, 25))
            throw new ArgumentException($"Invalid worker lane format. Only letters, numbers, underscore (_), hyphen (-) are allowed. Length must be between 1 and 25. Received: '{workerLane}'", nameof(workerLane));

        if (!string.IsNullOrEmpty(workerName) && !JobMasterStringUtils.IsValidForSegment(workerName!, 25))
            throw new ArgumentException($"Invalid worker name format. Only letters, numbers, underscore (_), hyphen (-) are allowed. Length must be between 1 and 25. Received: '{workerName}'", nameof(workerName));
        
        hostId ??= await masterHostService.RegisterNewHostAsync();
        var workerId = string.Empty;
        if (string.IsNullOrEmpty(workerName))
        {
            workerName = hostId.HostNameSanitized;
            workerName += $"-{JobMasterIdGenUtil.TimestampId()}";
            workerId = $"{hostId.IdValue}:{JobMasterIdGenUtil.NewShortId()}";
        }
        else
        {
            workerName += $"-{JobMasterIdGenUtil.TimestampId()}";
            workerId = $"{workerName}:{JobMasterIdGenUtil.NewShortId()}";
        }
        
        if (!JobMasterStringUtils.IsValidForId(workerId))
            throw new ArgumentException($"Invalid worker ID format. Only letters, numbers, underscore (_), hyphen (-), dot(.), and colon (:) are allowed. Received: '{workerId}'", nameof(workerName));
        
        var worker = new AgentWorkerRecord(ClusterConnConfig.ClusterId)
        {
            AgentConnectionId = agentConnectionId ?? string.Empty,
            Name = workerName,
            Id = workerId,
            Mode = mode,
            WorkerLane = workerLane,
            CreatedAt = DateTime.UtcNow,
            HostId = hostId.IdValue,
            HostDisplayName = hostId.HostDisplayName,
            ParallelismFactor = parallelismFactor
        };
        
        if (!worker.ToModel(DateTime.UtcNow, true).IsValid())
            throw new ArgumentException($"Worker configuration is invalid. WorkerName='{workerName}'", nameof(workerName));
        
        return worker;
    }
    
    private void NotifyChanges()
    {
        jobMasterInMemoryCache.Remove(cacheKeys.AllAgentsWorkers());
        masterChangesSentinelService.NotifyChanges(sentinelKeys.AgentsAndWorkers());
    }
    
    private class AgentWorkerRecord : JobMasterBaseModel
    {
        public AgentWorkerRecord(string clusterId) : base(clusterId)
        {
        }
        
        protected AgentWorkerRecord() {}

        public string Id { get; set; } = string.Empty;
        public string AgentConnectionId { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
        
        public AgentWorkerMode Mode { get; set; } = AgentWorkerMode.Full;
        
        public string? WorkerLane { get; set; }
        
        public double ParallelismFactor { get; set; } = 1;
        
        public DateTime? StopRequestedAt { get; set; }
        public TimeSpan? StopGracePeriod { get; set; }
        
        public string HostId { get; set; } = null!;
        
        public string HostDisplayName { get; set; } = null!;

        public AgentWorkerModel ToModel(DateTime lastHeartbeat, bool isAlive)
        {
            return new AgentWorkerModel(this.ClusterId)
            {
                Id = Id,
                AgentConnectionId = string.IsNullOrEmpty(AgentConnectionId) ? null : new AgentConnectionId(AgentConnectionId),
                Name = Name,
                LastHeartbeat = lastHeartbeat,
                IsAlive = isAlive,
                CreatedAt = CreatedAt,
                WorkerLane = WorkerLane,
                Mode = Mode,
                ParallelismFactor = ParallelismFactor,
                HostId = Abstractions.Models.Hosts.HostId.Recover(HostDisplayName, HostId),
            };
        }
    }
}