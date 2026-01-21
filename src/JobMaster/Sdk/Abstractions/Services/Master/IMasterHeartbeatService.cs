using JobMaster.Sdk.Abstractions.Ioc.Markups;

namespace JobMaster.Sdk.Abstractions.Services.Master;

public interface IMasterHeartbeatService : IJobMasterClusterAwareService
{
    void Heartbeat(string agentWorkerId);
    DateTime? GetLastHeartbeat(string agentWorkerId);
    IDictionary<string, DateTime?> GetLastHeartbeats(IList<string> agentWorkerIds);
}