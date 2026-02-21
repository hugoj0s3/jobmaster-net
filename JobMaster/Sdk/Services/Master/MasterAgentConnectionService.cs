using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.LocalCache;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Services.Master;

internal class MasterAgentConnectionService : JobMasterClusterAwareComponent, IMasterAgentConnectionService
{
    private readonly IMasterGenericRecordRepository masterGenericRecordRepository;
    private readonly JobMasterInMemoryKeys cacheKeys;
    private readonly JobMasterSentinelKeys sentinelKeys;
    private readonly IJobMasterInMemoryCache jobMasterInMemoryCache;
    private readonly IMasterHeartbeatService masterHeartbeatService;
    private readonly IMasterBucketsService masterBucketsService;

    public MasterAgentConnectionService(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IMasterGenericRecordRepository masterGenericRecordRepository,
        JobMasterInMemoryKeys cacheKeys,
        JobMasterSentinelKeys sentinelKeys,
        IJobMasterInMemoryCache jobMasterInMemoryCache,
        IMasterHeartbeatService masterHeartbeatService,
        IMasterBucketsService masterBucketsService) : base(clusterConnConfig)
    {
        this.masterGenericRecordRepository = masterGenericRecordRepository;
        this.cacheKeys = cacheKeys;
        this.sentinelKeys = sentinelKeys;
        this.jobMasterInMemoryCache = jobMasterInMemoryCache;
        this.masterHeartbeatService = masterHeartbeatService;
        this.masterBucketsService = masterBucketsService;
    }

    public async Task<AgentConnectionModel> SaveConnectionAsync(AgentConnectionId agentConnectionId, string footprint)
    {
        var agentConnectionRecord = await GetRecordAsync(agentConnectionId);
        if (agentConnectionRecord is null)
        {
            agentConnectionRecord = new AgentConnectionRecord(ClusterConnConfig.ClusterId)
            {
                Id = agentConnectionId.IdValue,
                Footprint = footprint,
                CreatedAt = DateTime.UtcNow,
                FootprintCreatedAt = DateTime.UtcNow,
            };
        }

        if (agentConnectionRecord.Footprint != footprint)
        {
            agentConnectionRecord.Footprint = footprint;
            agentConnectionRecord.FootprintCreatedAt = DateTime.UtcNow;
        }

        var record = GenericRecordEntry
            .Create(ClusterConnConfig.ClusterId, MasterGenericRecordGroupIds.AgentConnection, agentConnectionRecord.Id,
                agentConnectionRecord);
        await masterGenericRecordRepository.UpsertAsync(record);

        var lastHeartbeat =
            masterHeartbeatService.GetLastHeartbeat(ResourceHeartbeatType.AgentConnection, agentConnectionId.IdValue);
        return ToModel(agentConnectionRecord, lastHeartbeat);
    }

    public async Task<IList<AgentConnectionModel>> QueryAllAsync(bool useCache = true)
    {
        IList<AgentConnectionRecord> records;
        if (useCache)
        {
            var cacheItem = this.jobMasterInMemoryCache.Get<IList<AgentConnectionRecord>>(cacheKeys.AgentConnections);
            records = cacheItem?.Value ?? await QueryAllAndCache();
        }
        else
        {
            records = await QueryAllAndCache();
        }

        var heartbeats = masterHeartbeatService.GetLastHeartbeats(
            ResourceHeartbeatType.AgentConnection,
            records.Select(x => x.Id).ToList());

        return records.Select(x => ToModel(x, heartbeats.GetOrDefault(x.Id))).ToList();
    }

    public async Task<bool> SafeDeleteConnection(AgentConnectionId agentConnectionId)
    {
        var buckets = await this.masterBucketsService.QueryAsync(new MasterBucketQueryCriteria()
        {
            AgentConnectionId = agentConnectionId.IdValue
        });

        if (buckets.Any())
        {
            return false;
        }

        await this.masterGenericRecordRepository.DeleteAsync(MasterGenericRecordGroupIds.AgentConnection,
            agentConnectionId.IdValue);
        return true;
    }

    public async Task<AgentConnectionModel?> GetConnectionAsync(AgentConnectionId agentConnectionId,
        bool useCache = true)
    {
        if (useCache)
        {
            var all = await this.QueryAllAsync(useCache);
            return all.FirstOrDefault(x => x.Id == agentConnectionId);
        }

        var agentConnectionRecord = await GetRecordAsync(agentConnectionId);
        if (agentConnectionRecord is null)
        {
            return null;
        }

        var lastHeartbeatAt =
            masterHeartbeatService.GetLastHeartbeat(ResourceHeartbeatType.AgentConnection, agentConnectionId.IdValue);

        return ToModel(agentConnectionRecord, lastHeartbeatAt);
    }

    private async Task<IList<AgentConnectionRecord>> QueryAllAndCache()
    {
        var records = await this.masterGenericRecordRepository.QueryAsync(MasterGenericRecordGroupIds.AgentConnection);
        var agentConnectionRecords = records.Select(x => x.ToObject<AgentConnectionRecord>()).ToList();
        jobMasterInMemoryCache.Set(cacheKeys.AgentConnections, agentConnectionRecords);

        return agentConnectionRecords;
    }


    private AgentConnectionModel ToModel(AgentConnectionRecord agentConnectionRecord, DateTime? lastHeartbeatAt)
    {
        AgentConnectionId agentConnectionId = new AgentConnectionId(agentConnectionRecord.Id);
        return new AgentConnectionModel(ClusterConnConfig.ClusterId)
        {
            Id = agentConnectionId,
            Footprint = agentConnectionRecord.Footprint,
            CreatedAt = agentConnectionRecord.CreatedAt,
            FootprintCreatedAt = agentConnectionRecord.FootprintCreatedAt,
            LastHeartbeatAt = lastHeartbeatAt,
        };
    }


    private async Task<AgentConnectionRecord?> GetRecordAsync(AgentConnectionId agentConnectionId)
    {
        var record = await this.masterGenericRecordRepository
            .GetAsync(MasterGenericRecordGroupIds.AgentConnection, agentConnectionId.IdValue);

        if (record is null)
        {
            return null;
        }

        var agentConnectionRecord = record.ToObject<AgentConnectionRecord>();
        return agentConnectionRecord;
    }

    private class AgentConnectionRecord : JobMasterBaseModel
    {
        public AgentConnectionRecord(string clusterId) : base(clusterId)
        {
        }

        public string Id { get; set; } = string.Empty;
        public string Footprint { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public DateTime FootprintCreatedAt { get; set; }
    }
}