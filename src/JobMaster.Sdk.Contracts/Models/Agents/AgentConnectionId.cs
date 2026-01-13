using System.Text.Json.Serialization;
using JobMaster.Sdk.Contracts.Config;

namespace JobMaster.Sdk.Contracts.Models.Agents;

public sealed class AgentConnectionId
{
    [JsonInclude]
    public string Name { get; internal set; } = null!;
    
    [JsonInclude]
    public string ClusterId { get; internal set; } = null!;
    
    public string IdValue => $"{ClusterId}:{Name}";
    
    public AgentConnectionId(string clusterId, string name)
    {
        ClusterId = clusterId;
        Name = name;
    }
    
    [JsonConstructor]
    internal AgentConnectionId()
    {
    }

    public AgentConnectionId(string id)
    {
        var parts = id.Split(':');
        if (parts.Length != 2)
        {
            throw new ArgumentException($"Invalid agent connection ID: {id}");
        }
        
        ClusterId = parts[0];
        Name = parts[1];
    }

    public bool IsActive()
    {
        var clusterConnection = JobMasterClusterConnectionConfig.TryGet(this.ClusterId);
        if (clusterConnection == null || !clusterConnection.IsActive)
        {
            return false;
        }

        return clusterConnection.GetAllAgentConnectionConfigs().Any(x => x.Name == this.Name);
    }
    
    public override string ToString()
    {
        return IdValue;
    }
}

public static class AgentConnectionIdExtensions
{
    public static bool IsNotNullAndActive(this AgentConnectionId? agentConnectionId)
    {
        return agentConnectionId != null && agentConnectionId.IsActive();
    }
}