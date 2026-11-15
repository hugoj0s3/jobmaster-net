using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

/// <summary>What <see cref="JobRetryDecider"/> settled on: retry after <see cref="Delay"/>, or stop early.</summary>
internal readonly struct JobRetryOutcome
{
    private JobRetryOutcome(TimeSpan delay, JobRetryResolution resolution, string? reason)
    {
        Delay = delay;
        Resolution = resolution;
        Reason = reason;
    }

    public bool IsStopEarly => Resolution == JobRetryResolution.InterceptorStoppedEarly;
    public TimeSpan Delay { get; }
    public JobRetryResolution Resolution { get; }
    public string? Reason { get; }

    public static JobRetryOutcome Retry(TimeSpan delay, JobRetryResolution resolution)
    {
        if (resolution == JobRetryResolution.InterceptorStoppedEarly)
            throw new ArgumentException("Retry needs a delay; use StopEarly to stop.", nameof(resolution));

        return new JobRetryOutcome(delay, resolution, null);
    }

    public static JobRetryOutcome StopEarly(string? reason)
    {
        return new JobRetryOutcome(TimeSpan.Zero, JobRetryResolution.InterceptorStoppedEarly, reason);
    }
}
