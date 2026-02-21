using System.Text.Json.Serialization;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Abstractions.Models.Hosts;

internal class HostId
{
    public HostId(string clusterId, string hostDisplayName)
    {
        ClusterId = clusterId;
        HostDisplayName = hostDisplayName;
        HostNameSanitized = JobMasterStringUtils.SanitizeForSegment(hostDisplayName, 25);
        InstanceId = JobMasterIdUtil.NewShortId();
    }
    
    [JsonConstructor]
    internal HostId()
    {
    }
    
    [JsonInclude]
    public string HostDisplayName { get; internal set; } = null!;
    
    [JsonInclude]
    public string HostNameSanitized { get; internal set; } = null!;
    
    [JsonInclude]
    public string ClusterId { get; internal set; } = null!;
    
    [JsonInclude]
    public string InstanceId { get; internal set; } = null!;
    
    [JsonInclude]
    public string IdValue => $"{ClusterId}:{HostNameSanitized}:{InstanceId}";
    
    public static HostId Recover(string hostDisplayName, string idValue)
    {
        var parts = idValue.Split(':');
        if (parts.Length != 3)
            throw new ArgumentException("Invalid host id value");
        
        var result = new HostId()
        {
            ClusterId = parts[0],
            HostNameSanitized = parts[1],
            InstanceId =  parts[2],
            HostDisplayName =  hostDisplayName
        };
        
        return result;
    }
}