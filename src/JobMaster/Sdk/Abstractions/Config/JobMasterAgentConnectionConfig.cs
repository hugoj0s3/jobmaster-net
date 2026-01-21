using JobMaster.Sdk.Abstractions.Models.Agents;

namespace JobMaster.Sdk.Abstractions.Config;

public class JobMasterAgentConnectionConfig
{
    public JobMasterAgentConnectionConfig(
        string clusterId,
        string name, 
        string connectionString, 
        string repositoryTypeId, 
        JobMasterConfigDictionary? additionalConnConfig = null,
        int? runtimeDbOperationThrottleLimit = null)
    {
        AdditionalConnConfig = additionalConnConfig ?? new JobMasterConfigDictionary();
        ClusterId = clusterId;
        Name = name;
        ConnectionString = connectionString;
        RepositoryTypeId = repositoryTypeId;
        Id = new AgentConnectionId(clusterId, name).IdValue;
        RuntimeDbOperationThrottleLimit = runtimeDbOperationThrottleLimit;
    }
    
    public JobMasterConfigDictionary AdditionalConnConfig { get; set; } = new();

    public string Id { get; private set; }
    public string ClusterId { get; private set; }
    public string Name { get; private set; }
    public string ConnectionString { get; private set; }
    public string RepositoryTypeId { get; private set; }
    
    public int? RuntimeDbOperationThrottleLimit { get; private set; }

    public void SetRuntimeDbOperationThrottleLimit(int? value)
    {
        this.RuntimeDbOperationThrottleLimit = value;
    }
}