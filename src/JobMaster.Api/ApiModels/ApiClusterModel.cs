using JobMaster.Abstractions.Models;

namespace JobMaster.Api.ApiModels;

/// <summary>Represents a JobMaster cluster configuration as returned by the API.</summary>
public class ApiClusterModel : ApiClusterBaseModel
{
    /// <summary>Repository type identifier (e.g. database driver) used by this cluster.</summary>
    public string RepositoryTypeId { get; set; } = string.Empty;
    /// <summary>Default maximum execution time per job.</summary>
    public TimeSpan DefaultJobTimeout { get; set; }
    /// <summary>Look-ahead window within which jobs are dispatched from master to a bucket.</summary>
    public TimeSpan TransientThreshold { get; set; }
    /// <summary>Default maximum number of automatic retries per failed job.</summary>
    public int DefaultMaxOfRetryCount { get; set; }
    /// <summary>Current operational mode of the cluster.</summary>
    public ClusterMode ClusterMode { get; set; }
    /// <summary>Maximum allowed byte size for job messages.</summary>
    public int MaxMessageByteSize { get; set; }
    /// <summary>IANA timezone identifier used for recurring schedule evaluation.</summary>
    public string IanaTimeZoneId { get; set; } = string.Empty;
    /// <summary>Data retention window; records older than this are eligible for cleanup.</summary>
    public TimeSpan? DataRetentionTtl { get; set; }
    /// <summary>Additional cluster-specific configuration key-value pairs.</summary>
    public IDictionary<string, object> AdditionalConfig { get; set; } = new Dictionary<string, object>();
}