namespace JobMaster.Sdk.Background;

internal static class JobMasterParallelUtil
{
   public static Task ForEachAsync<TSource>(
        IEnumerable<TSource> source,
        ParallelOptions parallelOptions,
        Func<TSource, CancellationToken, ValueTask> body)
    {
        if (source == null) throw new ArgumentNullException(nameof(source));
        if (parallelOptions == null) throw new ArgumentNullException(nameof(parallelOptions));
        if (body == null) throw new ArgumentNullException(nameof(body));

#if NET6_0_OR_GREATER
        return Parallel.ForEachAsync(source, parallelOptions, body);
#else
        return ForEachAsyncLegacy(source, parallelOptions, body);
#endif
    }

#if !NET6_0_OR_GREATER
    private static async Task ForEachAsyncLegacy<TSource>(
        IEnumerable<TSource> source,
        ParallelOptions parallelOptions,
        Func<TSource, CancellationToken, ValueTask> body)
    {
        // If MaxDegreeOfParallelism is -1 (default), use int.MaxValue (no limit)
        // Otherwise, respect the configured limit.
        var maxDegree = parallelOptions.MaxDegreeOfParallelism == -1 
            ? int.MaxValue 
            : parallelOptions.MaxDegreeOfParallelism;

        using var semaphore = new SemaphoreSlim(maxDegree);
        var tasks = new List<Task>();
        var cancellationToken = parallelOptions.CancellationToken;

        // Same contract as Parallel.ForEachAsync: on cancellation, start no new item, wait for the running ones, then
        // throw -- callers act on what didn't run, so they mustn't see a cancellation while items still run (and the
        // semaphore mustn't be disposed under them).
        try
        {
            foreach (var item in source)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                // Wait for a semaphore slot before starting the next task
                await semaphore.WaitAsync(cancellationToken);

                var task = Task.Run(async () =>
                {
                    try
                    {
                        // Execute the body. AsTask() is necessary because the delegate returns ValueTask
                        await body(item, cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        // Release the slot for the next item
                        semaphore.Release();
                    }
                }, cancellationToken);

                tasks.Add(task);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Cancelled while waiting for a slot: still wait for the running items below.
        }

        // Wait for the running items and propagate their exceptions; an item cancelled before it started just
        // completes as cancelled.
        try
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }

        cancellationToken.ThrowIfCancellationRequested();
    }
#endif
}