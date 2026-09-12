using System.Text.Json.Serialization;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Abstractions.Models;

internal abstract class JobMasterBaseModel
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