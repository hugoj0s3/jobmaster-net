namespace JobMaster.Sdk.Abstractions.Config;

/// <summary>How a failed attempt's retry was settled: where the delay came from, or that it stopped early.</summary>
internal enum JobRetryResolution
{
    /// <summary>The cluster's RetryDelays, no interceptor configured.</summary>
    RetryDelays,

    /// <summary>The cluster's RetryDelays, chosen by the interceptor (<c>UseRetryDelays</c>).</summary>
    InterceptorUsedRetryDelays,

    /// <summary>The interceptor's own delay (<c>RetryAfter</c>).</summary>
    InterceptorCustomDelay,

    /// <summary>The interceptor's delay was under the minimum and was raised to it.</summary>
    InterceptorBelowMinimum,

    /// <summary>The interceptor threw or timed out; the cluster's RetryDelays were used instead.</summary>
    InterceptorFailed,

    /// <summary>The interceptor stopped the retries (<c>StopEarly</c>); no delay.</summary>
    InterceptorStoppedEarly,
}
