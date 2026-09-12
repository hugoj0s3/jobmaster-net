using JobMaster.Sdk.Abstractions.Ioc.Markups;

namespace JobMaster.Sdk.Abstractions.Services.Master;

internal interface IMasterHeartbeatService : IJobMasterClusterAwareService
{
    void Heartbeat(ResourceHeartbeatType type, string resourceId);
    DateTime? GetLastHeartbeat(ResourceHeartbeatType type, string resourceId);
    IDictionary<string, DateTime?> GetLastHeartbeats(ResourceHeartbeatType type, IList<string> resourceIds);
}

internal enum ResourceHeartbeatType
{
    Host = 1,
    AgentWorker,
    AgentConnection
}