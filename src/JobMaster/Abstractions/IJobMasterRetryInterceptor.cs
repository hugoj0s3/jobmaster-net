using JobMaster.Abstractions.Models;

namespace JobMaster.Abstractions;

/// <summary>
/// Intercepts the retry of a failed job attempt: retry after another delay, keep the cluster's <c>RetryDelays</c>, or
/// stop and fail the job now. It never adds attempts: <c>MaxNumberOfRetries</c> still caps them. Configured per
/// cluster with <c>InterceptRetry&lt;T&gt;()</c>; resolved from DI in a new scope for each failed attempt, so scoped
/// dependencies (e.g. a database context) work.
/// </summary>
public interface IJobMasterRetryInterceptor
{
    /// <summary>
    /// Called after a failed attempt that still has retries left (never once <c>MaxNumberOfRetries</c> is reached:
    /// the interceptor can stop earlier, not retry more). A delay under <see cref="JobMasterLimits.MinRetryDelay"/>
    /// is raised to it. If this throws, or doesn't return within <see cref="JobMasterLimits.RetryInterceptorTimeout"/>
    /// (<paramref name="cancellationToken"/> is cancelled then), the job is retried using the cluster's
    /// <c>RetryDelays</c>.
    /// </summary>
    Task<JobRetryDecision> DecideAsync(JobRetryContext context, CancellationToken cancellationToken);
}
