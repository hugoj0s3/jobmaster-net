using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services.Master;

internal class MasterHeartbeatService : JobMasterClusterAwareComponent, IMasterHeartbeatService
{
    private IMasterGenericRecordRepository repository = null!;

    public MasterHeartbeatService(JobMasterClusterConnectionConfig clusterConnectionConfig, IMasterGenericRecordRepository repository) : base(clusterConnectionConfig)
    {
        this.repository = repository;
    }

    public void Heartbeat(ResourceHeartbeatType type, string resourceId)
    {
        var agentWorkerHeartbeatRecord = new AgentWorkerHeartbeatRecord(ClusterConnConfig.ClusterId)
        {
            AgentWorkerId = resourceId,
            HeartbeatAt = DateTime.UtcNow,
        };
        
        var groupId = type switch
        {
            ResourceHeartbeatType.AgentWorker => MasterGenericRecordGroupIds.AgentWorkerHeartbeat,
            ResourceHeartbeatType.Host => MasterGenericRecordGroupIds.HostHeartbeat,
            ResourceHeartbeatType.AgentConnection => MasterGenericRecordGroupIds.AgentConnectionHeartbeat,
            _ => throw new ArgumentOutOfRangeException(nameof(type), $"Not expected type: {type}")
        };
        
        var record = GenericRecordEntry.Create(
            ClusterConnConfig.ClusterId, 
            groupId, 
            resourceId,
            agentWorkerHeartbeatRecord, 
            expiresAt: DateTime.UtcNow.AddDays(2));

        repository.Upsert(record);
    }

    public DateTime? GetLastHeartbeat(ResourceHeartbeatType type, string resourceId)
    {
        var groupId = type switch
        {
            ResourceHeartbeatType.AgentWorker => MasterGenericRecordGroupIds.AgentWorkerHeartbeat,
            ResourceHeartbeatType.Host => MasterGenericRecordGroupIds.HostHeartbeat,
            ResourceHeartbeatType.AgentConnection => MasterGenericRecordGroupIds.AgentConnectionHeartbeat,
            _ => throw new ArgumentOutOfRangeException(nameof(type), $"Not expected type: {type}")
        };
        
        var record = repository.Get(groupId, resourceId);
        return record?.ToObject<AgentWorkerHeartbeatRecord>()?.HeartbeatAt;
    }

    public IDictionary<string, DateTime?> GetLastHeartbeats(ResourceHeartbeatType type, IList<string> resourceIds)
    {
        var criteria = new GenericRecordQueryCriteria()
        {
            EntryIds = resourceIds,
        };
        
        var groupId = type switch
        {
            ResourceHeartbeatType.AgentWorker => MasterGenericRecordGroupIds.AgentWorkerHeartbeat,
            ResourceHeartbeatType.Host => MasterGenericRecordGroupIds.HostHeartbeat,
            ResourceHeartbeatType.AgentConnection => MasterGenericRecordGroupIds.AgentConnectionHeartbeat,
            _ => throw new ArgumentOutOfRangeException(nameof(type), $"Not expected type: {type}")
        };
        
        var records = repository.Query(groupId, criteria);
        return records.ToDictionary(x => x.EntryId, x => x.ToObject<AgentWorkerHeartbeatRecord>()?.HeartbeatAt);
    }
    
    private class AgentWorkerHeartbeatRecord : JobMasterBaseModel
    {
        public AgentWorkerHeartbeatRecord(string clusterId) : base(clusterId)
        {
        }
        
        protected AgentWorkerHeartbeatRecord() {}

        public string AgentWorkerId { get; set; } = string.Empty;
        public DateTime HeartbeatAt { get; set; }
    }
}