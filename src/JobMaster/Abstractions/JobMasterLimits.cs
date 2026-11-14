namespace JobMaster.Abstractions;

/// <summary>
/// Fixed limits JobMaster enforces; unlike <see cref="JobMasterDefaults"/>, they can't be configured.
/// Referenced by selector XML documentation so the documented limits match the enforced ones.
/// </summary>
public static class JobMasterLimits
{
    /// <summary>Shortest wait before a retry: 30 seconds. <c>RetryDelays</c> can't go lower; a retry interceptor's shorter delay is raised to it.</summary>
    public static readonly TimeSpan MinRetryDelay = TimeSpan.FromSeconds(30);

    /// <summary>How long a retry interceptor has to decide: 10 seconds. After that, the cluster's <c>RetryDelays</c> are used.</summary>
    public static readonly TimeSpan RetryInterceptorTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Minimum accepted positive data retention TTL: 10 minutes. Cleanup runners check at TTL/2, so the minimum check interval is 5 minutes.</summary>
    public static readonly TimeSpan MinDataRetentionTtl = TimeSpan.FromMinutes(10);
}
