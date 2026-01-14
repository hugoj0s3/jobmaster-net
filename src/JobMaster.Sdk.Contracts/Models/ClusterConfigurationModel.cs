using JobMaster.Contracts.Models;
using JobMaster.Contracts.Utils;
using JobMaster.Sdk.Contracts.Config;

namespace JobMaster.Sdk.Contracts.Models;

public class ClusterConfigurationModel : JobMasterBaseModel
{
    public ClusterConfigurationModel(string clusterId) : base(clusterId)
    {
    }
    
    protected ClusterConfigurationModel() {}

    public TimeSpan DefaultJobTimeout { get; set; } = TimeSpan.FromMinutes(5);
    
    public TimeSpan TransientThreshold { get; set; } = TimeSpan.FromMinutes(10);
    public int DefaultMaxOfRetryCount { get; set; } = 3;
    
    public ClusterMode ClusterMode { get; set; } = ClusterMode.Active;
    
    /// <summary>
    /// Maximum allowed size (in bytes) for a job dispatch payload (serialized JSON).
    /// This should be set slightly below the broker/repository hard limit to provide a safety margin
    /// for JSON escaping and protocol overhead (headers, metadata).
    /// 
    /// Guidance:
    /// - If your broker/repository limit is 1 MiB (1_048_576 bytes), set this to ~950 KiB (972_800 bytes).
    /// - If your broker/repository limit is 512 KiB (524_288 bytes), set this to ~480 KiB (491_520 bytes).
    ///   on your headers/observed overhead.
    /// 
    /// Special values:
    /// - -1 means unlimited: no application-side size enforcement. Use with caution; downstream transports
    ///   (Kafka/Redis/HTTP/etc.) may still reject large messages. Prefer a high but finite value in constrained environments.
    ///
    /// Enforcement:
    /// - Heuristic-only: CalcEstimateByteSize() is used before dispatch for performance reasons.
    ///   Keep a safety margin between this setting and the true transport limit to avoid rejections.
    /// </summary>
    public int MaxMessageByteSize { get; set; } = 128 * 1024;

    public string IanaTimeZoneId { get; set; } = TimeZoneUtils.GetLocalIanaTimeZoneId();
    
    /// <summary>
    /// Single cluster-wide data retention window (TTL).
    /// Controls how long executed jobs, inactive recurring schedules, and JobMaster logs are retained
    /// to keep dashboards consistent (e.g., failed jobs remain alongside their logs).
    ///
    /// Special values:
    /// - null = keep data forever (no automatic purge)
    /// </summary>
    public TimeSpan? DataRetentionTtl { get; set; } = TimeSpan.FromDays(30);

    public JobMasterConfigDictionary AdditionalConfig { get; set; } = new();

    public override bool IsValid()
    {
        return base.IsValid() && TimeZoneUtils.IsValidIanaId(IanaTimeZoneId);
    }
}