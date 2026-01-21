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
        // Se MaxDegreeOfParallelism for -1 (padrão), usamos int.MaxValue (sem limite)
        // Caso contrário, respeitamos o limite configurado.
        var maxDegree = parallelOptions.MaxDegreeOfParallelism == -1 
            ? int.MaxValue 
            : parallelOptions.MaxDegreeOfParallelism;

        using var semaphore = new SemaphoreSlim(maxDegree);
        var tasks = new List<Task>();
        var cancellationToken = parallelOptions.CancellationToken;

        foreach (var item in source)
        {
            // Para o loop se o token for cancelado
            if (cancellationToken.IsCancellationRequested)
            {
                break;
            }

            // Espera uma vaga no semáforo antes de iniciar a próxima task
            await semaphore.WaitAsync(cancellationToken);

            var task = Task.Run(async () =>
            {
                try
                {
                    // Executa o corpo. O AsTask() é necessário pois o delegate retorna ValueTask
                    await body(item, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    // Libera a vaga para o próximo item
                    semaphore.Release();
                }
            }, cancellationToken);

            tasks.Add(task);
        }

        // Aguarda todas as tasks terminarem e propaga exceções se houver
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }
#endif
}