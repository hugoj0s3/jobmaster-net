using JobMaster.Sdk.Contracts.Ioc.Markups;

namespace JobMaster.Sdk.Contracts.Services.Master;

public interface IMasterHeartbeatService : IJobMasterClusterAwareService
{
    void Heartbeat(string agentWorkerId);
    DateTime? GetLastHeartbeat(string agentWorkerId);
    IDictionary<string, DateTime?> GetLastHeartbeats(IList<string> agentWorkerIds);
}