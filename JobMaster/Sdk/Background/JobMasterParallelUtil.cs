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

        foreach (var item in source)
        {
            // Stop the loop if the token is cancelled
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

        // Wait for all tasks to complete and propagate exceptions if any
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }
#endif
}