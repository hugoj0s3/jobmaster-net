using System.Text.Json.Serialization;
using JobMaster.Internals;

namespace JobMaster.Sdk.Abstractions.Models;

public abstract class JobMasterBaseModel
{
    [JsonInclude]
    public string ClusterId { get; protected set; } = string.Empty;

    protected JobMasterBaseModel(string clusterId)
    {
        ClusterId = clusterId;
    }
    
    protected JobMasterBaseModel() {}
    
    public virtual bool IsValid() => JobMasterStringUtils.IsValidForSegment(ClusterId);
}