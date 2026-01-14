using JobMaster.Contracts.Extensions;
using JobMaster.Contracts.Models;
using JobMaster.Contracts.Utils;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.LocalCache;
using JobMaster.Sdk.Contracts.Models;
using JobMaster.Sdk.Contracts.Models.Agents;
using JobMaster.Sdk.Contracts.Models.GenericRecords;
using JobMaster.Sdk.Contracts.Repositories.Master;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services.Master;

public class MasterAgentWorkersService : JobMasterClusterAwareComponent, IMasterAgentWorkersService
{
    private IMasterClusterConfigurationService masterClusterConfigurationService = null!;
    private IMasterChangesSentinelService masterChangesSentinelService = null!;
    private IMasterHeartbeatService masterHeartbeatService = null!;
    private IMasterGenericRecordRepository masterGenericRecordRepository = null!;
    
    private IJobMasterInMemoryCache jobMasterInMemoryCache = null!;
    private JobMasterInMemoryKeys cacheKeys = null!;
    private JobMasterSentinelKeys sentinelKeys = null!;

    public MasterAgentWorkersService(
        JobMasterClusterConnectionConfig clusterConnectionConfig, 
        IJobMasterInMemoryCache jobMasterInMemoryCache,
        IMasterClusterConfigurationService masterClusterConfigurationService,
        IMasterChangesSentinelService masterChangesSentinelService,
        IMasterHeartbeatService masterHeartbeatService,
        IMasterGenericRecordRepository masterGenericRecordRepository) : base(clusterConnectionConfig)
    {
        this.jobMasterInMemoryCache = jobMasterInMemoryCache;
        this.masterClusterConfigurationService = masterClusterConfigurationService;
        this.masterChangesSentinelService = masterChangesSentinelService;
        this.masterHeartbeatService = masterHeartbeatService;
        this.masterGenericRecordRepository = masterGenericRecordRepository;

        cacheKeys = new JobMasterInMemoryKeys(clusterConnectionConfig.ClusterId);
        sentinelKeys = new JobMasterSentinelKeys(clusterConnectionConfig.ClusterId);
    }
    
    public IList<AgentWorkerModel> GetWorkers(string? agentConnectionId = null, bool useCache = true)
    {
        var all = GetAllAgentWorkers(useCache);
        return ToModel(all.Where(x => agentConnectionId == null || x.AgentConnectionId == agentConnectionId).ToList());
    }

    public async Task<IList<AgentWorkerModel>> GetWorkersAsync(string? agentConnectionId = null, bool useCache = true)
    {
        var all = await GetAllAgentWorkersAsync(useCache);
        return ToModel(all.Where(x => agentConnectionId == null || x.AgentConnectionId == agentConnectionId).ToList());
    }

    public AgentWorkerModel? GetWorker(string workerId)
    {
        var all = GetAllAgentWorkers();
        var worker = all.FirstOrDefault(x => x.Id == workerId);

        return ToModel(worker);
    }

    public async Task<AgentWorkerModel?> GetWorkerAsync(string workerId)
    {
        var all = await GetAllAgentWorkersAsync();
        var worker = all.FirstOrDefault(x => x.Id == workerId);

        return ToModel(worker);
    }

    public async Task<string> RegisterWorkerAsync(string agentConnectionId, string workerName, string? workerLane, AgentWorkerMode mode, double parallelismFactor)
    {
        var worker = CreateValidatedWorker(agentConnectionId, workerName, workerLane, mode, parallelismFactor);
        await masterGenericRecordRepository.InsertAsync(GenericRecordEntry.Create(ClusterConnConfig.ClusterId, MasterGenericRecordGroupIds.AgentWorker, workerName, worker));
        NotifyChanges();
        
        return worker.Id;
    }

    public string RegisterWorker(string agentConnectionId, string workerName, string? workerLane, AgentWorkerMode mode, double parallelismFactor)
    {
        var worker = CreateValidatedWorker(agentConnectionId, workerName, workerLane, mode, parallelismFactor);
        masterGenericRecordRepository.Insert(GenericRecordEntry.Create(ClusterConnConfig.ClusterId, MasterGenericRecordGroupIds.AgentWorker, worker.Id, worker));
        NotifyChanges();
        
        return worker.Id;
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
        
        await masterGenericRecordRepository.DeleteAsync(MasterGenericRecordGroupIds.AgentWorker, workerId);
    }

    private IList<AgentWorkerRecord> GetAllAgentWorkers(bool useCache = true)
    {
        var cacheKey = cacheKeys.AllAgentsWorkers();
        var sentinelKey = sentinelKeys.AgentsAndWorkers();

        var inCacheValue = jobMasterInMemoryCache.Get<IList<AgentWorkerRecord>>(cacheKey);
        if (inCacheValue == null ||
            inCacheValue.Value == null ||
            masterChangesSentinelService.HasChangesAfter(sentinelKey, inCacheValue.CreatedAt) ||
            !useCache)
        {
            var allWorkerRecords = this.masterGenericRecordRepository.Query(MasterGenericRecordGroupIds.AgentWorker);
            var allAgentWorkers = Enumerable.Select(allWorkerRecords, x => x.ToObject<AgentWorkerRecord>()).ToList<AgentWorkerRecord>();

            jobMasterInMemoryCache.Set(cacheKey, allAgentWorkers);
            return allAgentWorkers;
        }

        return inCacheValue.Value;
    }

    private async Task<IList<AgentWorkerRecord>> GetAllAgentWorkersAsync(bool useCache = true)
    {
        var cacheKey = cacheKeys.AllAgentsWorkers();
        var sentinelKey = sentinelKeys.AgentsAndWorkers();

        var inCacheValue = jobMasterInMemoryCache.Get<IList<AgentWorkerRecord>>(cacheKey);
        if (inCacheValue == null ||
            inCacheValue.Value == null ||
            masterChangesSentinelService.HasChangesAfter(sentinelKey, inCacheValue.CreatedAt) ||
            !useCache)
        {
            var allWorkerRecords = await this.masterGenericRecordRepository.QueryAsync(MasterGenericRecordGroupIds.AgentWorker);
            var allAgentWorkers = Enumerable.Select(allWorkerRecords, x => x.ToObject<AgentWorkerRecord>()).ToList<AgentWorkerRecord>();

            jobMasterInMemoryCache.Set(cacheKey, allAgentWorkers);
            return allAgentWorkers;
        }

        return inCacheValue.Value;
    }
    
    private AgentWorkerModel? ToModel(AgentWorkerRecord? worker)
    {
        if (worker == null)
        {
            return null;
        }

        return ToModel(new List<AgentWorkerRecord>() { worker }).First();
    }

    private IList<AgentWorkerModel> ToModel(IList<AgentWorkerRecord> workers)
    {
        var heartbeats = masterHeartbeatService.GetLastHeartbeats(workers.Select(x => x.Id).ToList());
        var heartbeatThreshold = JobMasterConstants.HeartbeatThreshold;

        return workers.Select(x =>
        {
            var lastHeartbeat = heartbeats.GetOrDefault<DateTime?>(x.Id) ?? x.CreatedAt;
            var isAlive = DateTime.UtcNow - lastHeartbeat < heartbeatThreshold;
            return x.ToModel(lastHeartbeat, isAlive);
        }).ToList();
    }
    
    private AgentWorkerRecord CreateValidatedWorker(
        string agentConnectionId, 
        string workerName, 
        string? workerLane, 
        AgentWorkerMode mode,
        double parallelismFactor)
    {
        if (workerLane != null && !JobMasterStringUtils.IsValidForSegment(workerLane, 25))
            throw new ArgumentException($"Invalid worker lane format. Only letters, numbers, underscore (_), hyphen (-), and dot (.) are allowed. Received: '{workerLane}'", nameof(workerLane));

        if (!JobMasterStringUtils.IsValidForId(workerName))
            throw new ArgumentException($"Invalid worker name format. Only letters, numbers, underscore (_), hyphen (-), dot(.), and colon (:) are allowed. Received: '{workerName}'", nameof(workerName));
        
        var workerId = $"{workerName}";
        if (!string.IsNullOrEmpty(workerLane))
        {
            workerId += $".{workerLane}";
        }

        workerId += $".{JobMasterIdUtil.NewShortId()}";
        
        
        if (!JobMasterStringUtils.IsValidForId(workerId))
            throw new ArgumentException($"Invalid worker ID format. Only letters, numbers, underscore (_), hyphen (-), dot(.), and colon (:) are allowed. Received: '{workerName}'", nameof(workerName));
        
        var worker = new AgentWorkerRecord(ClusterConnConfig.ClusterId)
        {
            AgentConnectionId = agentConnectionId,
            Name = workerName,
            Id = workerId,
            Mode = mode,
            WorkerLane = workerLane,
            CreatedAt = DateTime.UtcNow,
            ParallelismFactor = parallelismFactor
        };
        
        if (!worker.ToModel(DateTime.UtcNow, true).IsValid())
            throw new ArgumentException($"Invalid worker ID format. Only letters, numbers, underscore (_), hyphen (-), and dot (.) are allowed. Received: '{workerName}'", nameof(workerName));
        
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
        
        public AgentWorkerMode Mode { get; set; } = AgentWorkerMode.Standalone;
        
        public string? WorkerLane { get; set; }
        
        public double ParallelismFactor { get; set; } = 1;

        public AgentWorkerModel ToModel(DateTime lastHeartbeat, bool isAlive)
        {
            return new AgentWorkerModel(this.ClusterId)
            {
                Id = Id,
                AgentConnectionId = new AgentConnectionId(AgentConnectionId),
                Name = Name,
                LastHeartbeat = lastHeartbeat,
                IsAlive = isAlive,
                CreatedAt = CreatedAt,
                WorkerLane = WorkerLane,
                Mode = Mode,
                ParallelismFactor = ParallelismFactor,
            };
        }
    }
}