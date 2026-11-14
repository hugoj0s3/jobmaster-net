namespace JobMaster.Sdk.Abstractions.Config;

/// <summary>Where a retry's delay came from; shown in the failed execution's message.</summary>
internal enum JobRetryDelaySource
{
    /// <summary>The cluster's RetryDelays, no interceptor configured.</summary>
    RetryDelays,

    /// <summary>The cluster's RetryDelays, chosen by the interceptor (<c>UseRetryDelays</c>).</summary>
    InterceptorUsedRetryDelays,

    /// <summary>The interceptor's own delay (<c>RetryAfter</c>).</summary>
    Interceptor,

    /// <summary>The interceptor's delay was under the minimum and was raised to it.</summary>
    InterceptorBelowMinimum,

    /// <summary>The interceptor threw or timed out; the cluster's RetryDelays were used instead.</summary>
    InterceptorFailed,
}
