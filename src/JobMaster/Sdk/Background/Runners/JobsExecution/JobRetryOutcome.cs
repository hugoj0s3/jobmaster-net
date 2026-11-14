using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

/// <summary>What <see cref="JobRetryDecider"/> settled on: retry after <see cref="Delay"/>, or stop early.</summary>
internal readonly struct JobRetryOutcome
{
    private JobRetryOutcome(bool stopEarly, TimeSpan delay, JobRetryDelaySource source, string? reason)
    {
        IsStopEarly = stopEarly;
        Delay = delay;
        Source = source;
        Reason = reason;
    }

    public bool IsStopEarly { get; }
    public TimeSpan Delay { get; }
    public JobRetryDelaySource Source { get; }
    public string? Reason { get; }

    public static JobRetryOutcome Retry(TimeSpan delay, JobRetryDelaySource source)
    {
        return new JobRetryOutcome(false, delay, source, null);
    }

    public static JobRetryOutcome StopEarly(string? reason)
    {
        return new JobRetryOutcome(true, TimeSpan.Zero, default, reason);
    }
}
