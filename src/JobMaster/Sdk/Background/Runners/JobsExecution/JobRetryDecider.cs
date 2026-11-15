using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

/// <summary>
/// Decides the retry of a failed attempt that still has retries left: runs the cluster's retry interceptor (delegate
/// or <see cref="IJobMasterRetryInterceptor"/> from a new DI scope, within
/// <see cref="JobMasterLimits.RetryInterceptorTimeout"/>), falls back to the cluster's RetryDelays when there's none
/// or it fails, and never returns a delay under <see cref="JobMasterLimits.MinRetryDelay"/>.
/// </summary>
internal sealed class JobRetryDecider
{
    private readonly JobMasterClusterConnectionConfig clusterConnConfig;
    private readonly IServiceProvider serviceProvider;
    private readonly IJobMasterLogger logger;
    private readonly TimeSpan interceptorTimeout;

    public JobRetryDecider(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IServiceProvider serviceProvider,
        IJobMasterLogger logger,
        TimeSpan? interceptorTimeout = null)
    {
        this.clusterConnConfig = clusterConnConfig;
        this.serviceProvider = serviceProvider;
        this.logger = logger;
        this.interceptorTimeout = interceptorTimeout ?? JobMasterLimits.RetryInterceptorTimeout;
    }

    /// <summary><paramref name="job"/> still has this failure uncounted (<c>NumberOfFailures</c> as before the attempt).</summary>
    public async Task<JobRetryOutcome> DecideAsync(JobRawModel job, Exception exception)
    {
        var settings = clusterConnConfig.RetrySettings;
        var numberOfFailures = job.NumberOfFailures + 1;

        if (settings.Interceptor == null && settings.InterceptorType == null)
        {
            return JobRetryOutcome.Retry(settings.DelayFor(numberOfFailures), JobRetryResolution.RetryDelays);
        }

        var decision = await RunInterceptorAsync(settings, job, numberOfFailures, exception);
        if (decision == null)
        {
            return JobRetryOutcome.Retry(settings.DelayFor(numberOfFailures), JobRetryResolution.InterceptorFailed);
        }

        switch (decision.Value.Kind)
        {
            case JobRetryDecisionKind.StopEarly:
                return JobRetryOutcome.StopEarly(decision.Value.Reason);
            case JobRetryDecisionKind.RetryAfter:
                return decision.Value.Delay < JobMasterLimits.MinRetryDelay
                    ? JobRetryOutcome.Retry(JobMasterLimits.MinRetryDelay, JobRetryResolution.InterceptorBelowMinimum)
                    : JobRetryOutcome.Retry(decision.Value.Delay, JobRetryResolution.InterceptorCustomDelay);
            default:
                return JobRetryOutcome.Retry(settings.DelayFor(numberOfFailures), JobRetryResolution.InterceptorUsedRetryDelays);
        }
    }

    // Null when the interceptor threw or timed out (logged here).
    private async Task<JobRetryDecision?> RunInterceptorAsync(JobMasterRetrySettings settings, JobRawModel job, int numberOfFailures, Exception exception)
    {
        try
        {
            var context = new JobRetryContext(ToCountedJobContext(job, numberOfFailures), exception);
            if (settings.Interceptor != null)
            {
                return settings.Interceptor(context);
            }

            return await RunInterceptorTypeAsync(settings.InterceptorType!, context);
        }
        catch (Exception e)
        {
            // Warn with the job: its attempt's failure is the Error there, and it still retries on RetryDelays.
            // Error with the cluster: the interceptor is the cluster's, so it's broken for every job on it.
            logger.Warn(
                $"Retry interceptor {settings.InterceptorDescription} failed; retrying with RetryDelays. See the cluster's logs for the error.",
                JobMasterLogCategory.JobExecution, job.Id);
            logger.Error(
                $"Retry interceptor {settings.InterceptorDescription} failed for job {job.Id.ToBase64()}; it was retried with RetryDelays.",
                JobMasterLogCategory.Cluster, clusterConnConfig.ClusterId, exception: e);
            return null;
        }
    }

    private async Task<JobRetryDecision> RunInterceptorTypeAsync(Type interceptorType, JobRetryContext context)
    {
        // The limit covers resolving the interceptor too (a constructor doing work counts).
        using var cts = new CancellationTokenSource(interceptorTimeout);
        await using var scope = serviceProvider.CreateAsyncScope();
        var interceptor = (IJobMasterRetryInterceptor)scope.ServiceProvider.GetRequiredService(interceptorType);

        var decisionTask = interceptor.DecideAsync(context, cts.Token);
        var timeoutTask = Task.Delay(Timeout.Infinite, cts.Token);
        if (await Task.WhenAny(decisionTask, timeoutTask) != decisionTask)
        {
            // An interceptor ignoring the token keeps running; observe its outcome so a late fault isn't unobserved.
            _ = decisionTask.ContinueWith(t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted);
            throw new TimeoutException($"Retry interceptor {interceptorType.Name} didn't decide within {interceptorTimeout}.");
        }

        return await decisionTask;
    }

    private static JobContext ToCountedJobContext(JobRawModel job, int numberOfFailures)
    {
        var counted = job.ToJob();
        counted.NumberOfFailures = numberOfFailures;
        return JobConvertUtil.ToJobContext(counted);
    }
}
