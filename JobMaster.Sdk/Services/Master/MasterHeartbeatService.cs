using JobMaster.Contracts.Models;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Models;
using JobMaster.Sdk.Contracts.Models.GenericRecords;
using JobMaster.Sdk.Contracts.Repositories.Master;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services.Master;

public class MasterHeartbeatService : JobMasterClusterAwareComponent, IMasterHeartbeatService
{
    private IMasterGenericRecordRepository repository = null!;

    public MasterHeartbeatService(JobMasterClusterConnectionConfig clusterConnectionConfig, IMasterGenericRecordRepository repository) : base(clusterConnectionConfig)
    {
        this.repository = repository;
    }

    public void Heartbeat(string agentWorkerId)
    {
        var agentWorkerHeartbeatRecord = new AgentWorkerHeartbeatRecord(ClusterConnConfig.ClusterId)
        {
            AgentWorkerId = agentWorkerId,
            HeartbeatAt = DateTime.UtcNow,
        };
        
        var record = GenericRecordEntry.Create(
            ClusterConnConfig.ClusterId, 
            MasterGenericRecordGroupIds.AgentWorkerHeartbeat, 
            agentWorkerId,
            agentWorkerHeartbeatRecord, 
            expiresAt: DateTime.UtcNow.AddDays(15));

        repository.Upsert(record);
    }

    public DateTime? GetLastHeartbeat(string agentWorkerId)
    {
        var record = repository.Get(MasterGenericRecordGroupIds.AgentWorkerHeartbeat, agentWorkerId);
        return record?.ToObject<AgentWorkerHeartbeatRecord>()?.HeartbeatAt;
    }

    public IDictionary<string, DateTime?> GetLastHeartbeats(IList<string> agentWorkerIds)
    {
        var criteria = new GenericRecordQueryCriteria()
        {
            EntryIds = agentWorkerIds,
        };
        
        var records = repository.Query(MasterGenericRecordGroupIds.AgentWorkerHeartbeat, criteria);
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