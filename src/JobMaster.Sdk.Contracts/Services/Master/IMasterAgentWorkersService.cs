using JobMaster.Contracts.Models;
using JobMaster.Sdk.Contracts.Ioc.Markups;
using JobMaster.Sdk.Contracts.Models.Agents;

namespace JobMaster.Sdk.Contracts.Services.Master;

public interface IMasterAgentWorkersService : IJobMasterClusterAwareService
{
    IList<AgentWorkerModel> GetWorkers(string? agentConnectionId = null, bool useCache = true);
    Task<IList<AgentWorkerModel>> GetWorkersAsync(string? agentConnectionId = null, bool useCache = true);
    
    AgentWorkerModel? GetWorker(string workerId);
    Task<AgentWorkerModel?> GetWorkerAsync(string workerId);
    Task<string> RegisterWorkerAsync(string agentConnectionId, string workerName, string? workerLane, AgentWorkerMode mode, double parallelismFactor);
    
    string RegisterWorker(string agentConnectionId, string workerName, string? workerLane, AgentWorkerMode mode, double parallelismFactor);
    
    void DeleteWorker(string workerId);
    Task DeleteWorkerAsync(string workerId);
    
    Task StopGracefulWorkerAsync(string workerId, TimeSpan? gracePeriod = null);
}