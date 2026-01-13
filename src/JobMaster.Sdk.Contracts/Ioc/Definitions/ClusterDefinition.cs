using JobMaster.Contracts.Models;
using JobMaster.Sdk.Contracts.Config;

namespace JobMaster.Sdk.Contracts.Ioc.Definitions;

public sealed class ClusterDefinition
{
    public string? ClusterId { get; set; }
    
    public string? RepoType { get; set; }
    public string? ConnString { get; set; }
    public JobMasterConfigDictionary? AdditionalConnConfig { get; set; }
    
    public JobMasterConfigDictionary? AdditionalConfig { get; set; }
    public int? DefaultMaxRetryCount { get; set; }
    public TimeSpan? DefaultJobTimeout { get; set; }
    public int? MaxMessageByteSize { get; set; }
    public string? IanaTimeZoneId { get; set; }
    public TimeSpan? TransientThreshold { get; set; }
    
    public int? DbOperationThrottleLimit { get; set; }
    
    public int? RuntimeDbOperationThrottleLimit { get; set; }
    
    public ClusterMode? ClusterMode { get; set; }
    
    public bool IsDefault { get; set; }
    
    public IList<AgentConnectionDefinition> AgentConnections { get; set; } = new List<AgentConnectionDefinition>();
    public IList<WorkerDefinition> Workers { get; set; } = new List<WorkerDefinition>();
}