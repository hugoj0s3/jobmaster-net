using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Agents;

namespace JobMaster.Sdk.Abstractions.Services.Master;

internal interface IMasterAgentConnectionService : IJobMasterClusterAwareService
{
    Task<AgentConnectionModel> SaveConnectionAsync(
        AgentConnectionId agentConnectionId, 
        string repositoryTypeId, 
        string fingerprint,
        bool protectChanges);
    Task<IList<AgentConnectionModel>> QueryAllAsync(bool useCache = true);
    Task<bool> SafeDeleteConnectionAsync(AgentConnectionId agentConnectionId);

    Task<bool> HasBucketsAsync(AgentConnectionId agentConnectionId);
    
    Task<AgentConnectionModel?> GetConnectionAsync(AgentConnectionId agentConnectionId, bool useCache = true);
}