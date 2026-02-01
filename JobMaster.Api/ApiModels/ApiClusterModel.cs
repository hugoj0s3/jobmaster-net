using JobMaster.Abstractions.Models;

namespace JobMaster.Api.ApiModels;

public class ApiClusterModel : ApiClusterBaseModel
{
    public string RepositoryTypeId { get; set; } = string.Empty;
    public TimeSpan DefaultJobTimeout { get; set; }
    public TimeSpan TransientThreshold { get; set; }
    public int DefaultMaxOfRetryCount { get; set; }
    public ClusterMode ClusterMode { get; set; }
    public int MaxMessageByteSize { get; set; }
    public string IanaTimeZoneId { get; set; } = string.Empty;
    public TimeSpan? DataRetentionTtl { get; set; }
    public IDictionary<string, object> AdditionalConfig { get; set; } = new Dictionary<string, object>();
}