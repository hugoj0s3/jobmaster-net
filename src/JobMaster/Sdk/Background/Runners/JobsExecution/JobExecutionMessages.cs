using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

/// <summary>
/// Text of a failed attempt, the same for every failure path: what happened, then what happens next, then (only when
/// the log has more, i.e. a stack trace) where to look. Used for the execution's <c>OutcomeMessage</c> (shown on the
/// attempt in the dashboard) and its JobExecution log line. Ids are base64, like everything users read.
/// </summary>
internal static class JobExecutionMessages
{
    // Keeps the attempt summary short; the full exception is in the log.
    private const int MaxExceptionMessageLength = 500;
    private const string SeeLogs = "See the job's logs for the full error.";

    /// <summary>e.g. <c>Failed after 120 ms: InvalidOperationException: Gateway down. Retrying in 00:02:00 (RetryDelays). See the job's logs for the full error.</c></summary>
    public static string HandlerFailed(long elapsedMs, Exception exception, string nextStep)
    {
        return $"Failed after {elapsedMs} ms: {DescribeException(exception)}. {nextStep} {SeeLogs}";
    }

    /// <summary>e.g. <c>Timed out after 61250 ms (timeout 00:01:00). Retrying in 00:00:30 (RetryDelays).</c></summary>
    public static string TimedOut(long elapsedMs, TimeSpan timeout, string nextStep)
    {
        return $"Timed out after {elapsedMs} ms (timeout {timeout:c}). {nextStep}";
    }

    /// <summary>The previous run of the same recurring schedule still holds its overlap lock; failed, not retried.</summary>
    public static string RecurringOverlap(Guid recurringScheduleId)
    {
        return $"Not run: recurring schedule {recurringScheduleId.ToBase64()} still has a run in progress. Failed, not retried.";
    }

    /// <summary>The job's recurring schedule no longer exists; failed, not retried.</summary>
    public static string RecurringScheduleNotFound(Guid recurringScheduleId)
    {
        return $"Not run: recurring schedule {recurringScheduleId.ToBase64()} was not found. Failed, not retried.";
    }

    /// <summary>The job changed on master during the run (another process is probably running it).</summary>
    public static string ConcurrentChange(JobMasterJobStatus currentStatus)
    {
        return $"Recorded as failed: the job changed on master during the run (now {currentStatus}); it's probably running in another process. {SeeLogs}";
    }

    private static string DescribeException(Exception exception)
    {
        var message = exception.Message.Replace("\r", " ").Replace("\n", " ").Trim();
        if (message.Length > MaxExceptionMessageLength)
        {
            message = message.Substring(0, MaxExceptionMessageLength) + "…";
        }

        return string.IsNullOrEmpty(message) ? exception.GetType().Name : $"{exception.GetType().Name}: {message.TrimEnd('.')}";
    }
}
