namespace JobMaster.Abstractions.Models;

/// <summary>
/// What a retry interceptor gets after a failed attempt: the job, how many times it has failed, and why.
/// </summary>
public sealed class JobRetryContext
{
    internal JobRetryContext(JobContext job, Exception exception)
    {
        Job = job;
        Exception = exception;
    }

    /// <summary>The failed job, with this failure already counted (<c>Job.NumberOfFailures</c> is 1 on the first failure).</summary>
    public JobContext Job { get; }

    /// <summary>Same as <c>Job.NumberOfFailures</c>: failures so far, this one included.</summary>
    public int NumberOfFailures => Job.NumberOfFailures;

    /// <summary>What failed the attempt: the handler's exception, or a <see cref="TimeoutException"/> when it exceeded its timeout.</summary>
    public Exception Exception { get; }
}
