using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Logs;

namespace JobMaster.Sdk.Abstractions.Ioc.Definitions;

internal sealed class ClusterDefinition
{
    public string? ClusterId { get; set; }

    public ISet<JobMasterPriority> DisabledPriorities { get; } = new HashSet<JobMasterPriority>();
    
    public string? RepoType { get; set; }
    public string? ConnString { get; set; }
    public JobMasterConfigDictionary? AdditionalConnConfig { get; set; }
    
    public JobMasterConfigDictionary? AdditionalConfig { get; set; }
    public int? DefaultMaxRetryCount { get; set; }
    public TimeSpan? DefaultJobTimeout { get; set; }
    public int? MaxMessageByteSize { get; set; }
    public string? IanaTimeZoneId { get; set; }
    public TimeSpan? TransientThreshold { get; set; }
    public TimeSpan? DataRetentionTtl { get; set; }

    public int? RuntimeDbOperationLimit { get; set; }
    
    public ClusterMode? ClusterMode { get; set; }
    
    public bool IsDefault { get; set; }
    
    public IList<AgentConnectionDefinition> AgentConnections { get; set; } = new List<AgentConnectionDefinition>();
    public IList<WorkerDefinition> Workers { get; set; } = new List<WorkerDefinition>();
    public Action<LogItem>? MirrorLog { get; set; }
    public string? MirrorLogFilePath { get; set; }
    public int? MirrorLogMaxBufferItems { get; set; }
    public TimeSpan? MirrorLogFlushInterval { get; set; }
    
    public bool? IsStandalone { get; set; }

    // Shared by IClusterConfigSelector.DataRetentionTtl and IClusterStandaloneConfigSelector.ClusterDataRetentionTtl —
    // both selectors mutate the same ClusterDefinition, so the validation belongs here instead of being
    // duplicated in both builder classes.
    public void SetDataRetentionTtl(TimeSpan dataRetentionTtl)
    {
        if (dataRetentionTtl > TimeSpan.Zero && dataRetentionTtl < JobMasterDefaults.MinDataRetentionTtl)
            throw new ArgumentException(
                $"DataRetentionTtl must be at least {JobMasterDefaults.MinDataRetentionTtl} " +
                $"or zero/negative for infinite retention. Use RetainDataForever() to disable purging.",
                nameof(dataRetentionTtl));
        DataRetentionTtl = dataRetentionTtl <= TimeSpan.Zero ? TimeSpan.Zero : dataRetentionTtl;
    }

    // Shared by IClusterConfigSelector.DisablePriority and IClusterStandaloneConfigSelector.DisablePriority.
    public void DisablePriority(JobMasterPriority priority)
    {
        if (priority == JobMasterPriority.Medium)
            throw new ArgumentException("Medium priority cannot be disabled.", nameof(priority));
        DisabledPriorities.Add(priority);
    }
}