using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Abstractions.Config;

/// <summary>A cluster's retry configuration: its delays (or the built-in ones) and at most one interceptor.</summary>
internal sealed class JobMasterRetrySettings
{
    public static readonly JobMasterRetrySettings BuiltIn = new(JobMasterDefaults.RetryDelays, interceptor: null, interceptorType: null);

    public JobMasterRetrySettings(
        IReadOnlyList<TimeSpan>? delays,
        Func<JobRetryContext, JobRetryDecision>? interceptor,
        Type? interceptorType)
    {
        Delays = delays ?? JobMasterDefaults.RetryDelays;
        Interceptor = interceptor;
        InterceptorType = interceptorType;
    }

    public const string NoInterceptor = "None";
    public const string DelegateInterceptor = "Delegate";

    public IReadOnlyList<TimeSpan> Delays { get; }
    public Func<JobRetryContext, JobRetryDecision>? Interceptor { get; }
    public Type? InterceptorType { get; }

    /// <summary>For display only (stored on master): <c>None</c>, <c>Delegate</c>, or the interceptor type's full name.</summary>
    public string InterceptorDescription => InterceptorType?.FullName ?? (Interceptor != null ? DelegateInterceptor : NoInterceptor);

    /// <summary>The delay before retry <paramref name="numberOfFailures"/> (1-based); the last delay repeats.</summary>
    public TimeSpan DelayFor(int numberOfFailures)
    {
        var index = JobMasterMathUtil.Clamp(numberOfFailures, 1, Delays.Count) - 1;
        return Delays[index];
    }

    // The texts below end the failed execution's message, so each attempt records what was actually applied
    // (the cluster-level stored settings only show the last worker that started).

    /// <summary>e.g. <c>Retrying in 00:02:00 (RetryDelays).</c></summary>
    public string RetryingLog(TimeSpan delay, JobRetryResolution resolution)
    {
        return $"Retrying in {delay:c} ({DescribeResolution(resolution)}).";
    }

    /// <summary>e.g. <c>Stopped early by retry interceptor MyApp.BillingRetryInterceptor: invalid input.</c></summary>
    public string StoppedEarlyLog(string? reason)
    {
        return string.IsNullOrEmpty(reason)
            ? $"Stopped early by retry interceptor {InterceptorDescription}."
            : $"Stopped early by retry interceptor {InterceptorDescription}: {reason}";
    }

    /// <summary>e.g. <c>Reached max retries (3).</c></summary>
    public string MaxRetriesReachedLog(int maxNumberOfRetries)
    {
        return $"Reached max retries ({maxNumberOfRetries}).";
    }

    /// <summary>
    /// When this worker's settings differ from the ones stored on master (written by the last worker that started),
    /// the log for replacing them; null when they're the same.
    /// </summary>
    public string? ReplacedStoredSettingsLog(IList<TimeSpan> storedDelays, string storedInterceptor)
    {
        if (storedDelays.SequenceEqual(Delays) && string.Equals(storedInterceptor, InterceptorDescription, StringComparison.Ordinal))
        {
            return null;
        }

        return $"Retry settings stored for the cluster changed from [{FormatDelays(storedDelays)}] / retry interceptor {storedInterceptor} " +
               $"to this worker's [{FormatDelays(Delays)}] / retry interceptor {InterceptorDescription}. Informational only: each " +
               "worker retries by its own configuration; if this repeats, workers are running different configurations.";
    }

    private static string FormatDelays(IEnumerable<TimeSpan> delays)
    {
        return string.Join(", ", delays.Select(d => d.ToString("c")));
    }

    private string DescribeResolution(JobRetryResolution resolution)
    {
        switch (resolution)
        {
            case JobRetryResolution.InterceptorUsedRetryDelays:
                return $"RetryDelays, chosen by retry interceptor {InterceptorDescription}";
            case JobRetryResolution.InterceptorCustomDelay:
                return $"retry interceptor {InterceptorDescription}";
            case JobRetryResolution.InterceptorBelowMinimum:
                return $"retry interceptor {InterceptorDescription}, raised to the {JobMasterLimits.MinRetryDelay:c} minimum";
            case JobRetryResolution.InterceptorFailed:
                return $"RetryDelays; retry interceptor {InterceptorDescription} failed";
            default:
                return "RetryDelays";
        }
    }
}
