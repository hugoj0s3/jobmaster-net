namespace JobMaster.Abstractions.Models;

/// <summary>
/// The outcome of a retry interceptor: <see cref="RetryAfter"/>, <see cref="UseRetryDelays"/> or <see cref="StopEarly"/>.
/// <c>default(JobRetryDecision)</c> is the same as <see cref="UseRetryDelays"/>.
/// </summary>
public readonly struct JobRetryDecision
{
    private JobRetryDecision(JobRetryDecisionKind kind, TimeSpan delay, string? reason)
    {
        Kind = kind;
        Delay = delay;
        Reason = reason;
    }

    internal JobRetryDecisionKind Kind { get; }
    internal TimeSpan Delay { get; }
    internal string? Reason { get; }

    /// <summary>Retry after <paramref name="delay"/> (raised to <see cref="JobMasterLimits.MinRetryDelay"/> if shorter).</summary>
    public static JobRetryDecision RetryAfter(TimeSpan delay)
    {
        return new JobRetryDecision(JobRetryDecisionKind.RetryAfter, delay, null);
    }

    /// <summary>Retry using the cluster's <c>RetryDelays</c>, or <see cref="JobMasterDefaults.RetryDelays"/> when none are set.</summary>
    public static JobRetryDecision UseRetryDelays()
    {
        return new JobRetryDecision(JobRetryDecisionKind.UseRetryDelays, TimeSpan.Zero, null);
    }

    /// <summary>
    /// Stop retrying before <c>MaxNumberOfRetries</c> is reached: fail the job now. <paramref name="reason"/> is added to
    /// the failed execution's message, shown in the dashboard.
    /// </summary>
    public static JobRetryDecision StopEarly(string? reason = null)
    {
        return new JobRetryDecision(JobRetryDecisionKind.StopEarly, TimeSpan.Zero, reason);
    }
}
