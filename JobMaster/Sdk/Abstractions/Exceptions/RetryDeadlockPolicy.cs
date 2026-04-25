using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Abstractions.Exceptions;

/// <summary>
/// Retries a delegate when its exception is identified as a database deadlock by the
/// configured <see cref="IKnownExceptionIdentifier"/>. Backs off between attempts using a
/// jittered delay to avoid thundering-herd retries when multiple workers are victims of
/// the same write.
/// </summary>
/// <remarks>
/// <para>
/// Provider neutrality is delegated to <see cref="IKnownExceptionIdentifier"/>: SQL Server
/// (1205), MySQL (1213), and Postgres (40P01) all map to
/// <see cref="JobMasterKnownExceptionId.Deadlock"/> via per-provider strategies, so call
/// sites stay portable. Only deadlock-classified exceptions are retried; everything else
/// rethrows immediately.
/// </para>
/// <para>
/// Backoff is constant-interval with up to 50% additive jitter (see
/// <see cref="JitteredDelay"/>). With <c>retryInterval = 250ms</c> and <c>maxRetryCount = 3</c>,
/// total worst-case wall time across retries is ~1.1 seconds. Tune the budgets relative to
/// any singleflight lock duration the caller imposes downstream — e.g. a
/// <c>valueFactoryLockDuration</c> on
/// <see cref="JobMaster.Sdk.Abstractions.LocalCache.IJobMasterInMemoryCache.GetOrSet{T}"/>
/// must comfortably exceed the worst-case retry chain.
/// </para>
/// <para>
/// The sync <see cref="Exec{T}(Func{T})"/> path uses <see cref="Thread.Sleep(TimeSpan)"/>
/// between attempts. This is acceptable for server-side background work — the deadlock
/// retry is the failure tail, not the hot path, and the alternatives (sync-over-async via
/// <c>Task.Delay().Wait()</c>, busy-spin via <see cref="SpinWait"/>) are strictly worse on
/// the dimensions that matter here. Prefer <see cref="ExecAsync{T}(Func{Task{T}})"/> from
/// async call paths.
/// </para>
/// </remarks>
internal class RetryDeadlockPolicy
{
    private readonly IKnownExceptionIdentifier knownExceptionIdentifier;
    private readonly TimeSpan retryInterval;
    private readonly int maxRetryCount;

    /// <summary>
    /// Creates a policy bound to a provider-aware exception identifier.
    /// </summary>
    /// <param name="knownExceptionIdentifier">Translates a thrown exception to a
    /// <see cref="JobMasterKnownExceptionId"/>. Must be cluster-scoped — the identifier
    /// resolves the active provider strategy from the bound cluster's repository type.</param>
    /// <param name="retryInterval">Base wait between attempts. Actual wait is
    /// <c>retryInterval + Random(0, retryInterval/2)</c> milliseconds (see
    /// <see cref="JitteredDelay"/>).</param>
    /// <param name="maxRetryCount">Maximum number of retries after the initial attempt. A
    /// value of 3 means up to 4 total invocations of the delegate.</param>
    public RetryDeadlockPolicy(
        IKnownExceptionIdentifier knownExceptionIdentifier,
        TimeSpan retryInterval,
        int maxRetryCount)
    {
        this.knownExceptionIdentifier = knownExceptionIdentifier;
        this.retryInterval = retryInterval;
        this.maxRetryCount = maxRetryCount;
    }

    /// <summary>
    /// Asynchronously runs <paramref name="func"/>, retrying on deadlocks with jittered
    /// non-blocking <see cref="Task.Delay(TimeSpan)"/> waits between attempts.
    /// </summary>
    /// <typeparam name="T">Result type produced by the delegate.</typeparam>
    /// <param name="func">Operation to execute. The same delegate is reinvoked on each
    /// retry, so it must be safe to run multiple times — typically the case for the kind
    /// of read or transactional write that surfaces a deadlock.</param>
    /// <returns>The first successful result.</returns>
    /// <exception cref="Exception">Rethrows the original exception once
    /// <see cref="maxRetryCount"/> is exhausted, or immediately if the exception is not
    /// classified as a deadlock.</exception>
    public async Task<T> ExecAsync<T>(Func<Task<T>> func)
    {
        var retryCount = 0;
        while (true)
        {
            try
            {
                return await func();
            }
            catch (Exception ex) when (ShouldRetry(ex, retryCount))
            {
                await Task.Delay(JitteredDelay());
                retryCount++;
            }
        }
    }

    /// <summary>
    /// Synchronously runs <paramref name="func"/>, retrying on deadlocks with jittered
    /// <see cref="Thread.Sleep(TimeSpan)"/> waits between attempts.
    /// </summary>
    /// <remarks>
    /// Reach for this overload only from sync call paths. The blocking sleep is bounded
    /// (see class-level remarks) but not free — async callers should prefer
    /// <see cref="ExecAsync{T}(Func{Task{T}})"/> to avoid tying up threads during waits.
    /// </remarks>
    public T Exec<T>(Func<T> func)
    {
        var retryCount = 0;
        while (true)
        {
            try
            {
                return func();
            }
            catch (Exception ex) when (ShouldRetry(ex, retryCount))
            {
                Thread.Sleep(JitteredDelay());
                retryCount++;
            }
        }
    }

    /// <summary>
    /// Convenience overload for sync delegates that don't return a value.
    /// </summary>
    public void Exec(Action action) => Exec(() =>
    {
        action();
        return true;
    });

    /// <summary>
    /// Convenience overload for async delegates that don't return a value.
    /// </summary>
    public Task ExecAsync(Func<Task> func) => ExecAsync(async () =>
    {
        await func();
        return true;
    });

    /// <summary>
    /// Decides whether the policy should retry. Returns <see langword="true"/> only when
    /// the exception is classified as a deadlock and the retry budget has not been
    /// exhausted; everything else short-circuits the loop and lets the exception
    /// propagate.
    /// </summary>
    private bool ShouldRetry(Exception exception, int retryCount)
    {
        if (knownExceptionIdentifier.Identify(exception) == JobMasterKnownExceptionId.Deadlock
            && retryCount < maxRetryCount)
        {
            return true;
        }

        return false;
    }

    /// <summary>
    /// Computes a delay equal to <c>retryInterval + jitter</c>, where jitter is a uniform
    /// random value in <c>[0, retryInterval/2]</c> milliseconds. The jitter scatters
    /// concurrent deadlock victims so they don't all retry in lockstep and re-collide on
    /// the same row/page lock that produced the original deadlock.
    /// </summary>
    private TimeSpan JitteredDelay()
    {
        var baseMs = (int)retryInterval.TotalMilliseconds;
        var jitterMs = JobMasterRandomUtil.GetInt(0, baseMs / 2 + 1);
        return TimeSpan.FromMilliseconds(baseMs + jitterMs);
    }
}
