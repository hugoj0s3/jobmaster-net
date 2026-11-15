namespace JobMaster.Sdk.Background;

/// <summary>
/// The process's single <see cref="AppDomain.ProcessExit"/> hook: runs the shutdown steps (stopping the workers)
/// first, then the final flushes (the loggers), so what the steps log is still written. Separate ProcessExit handlers
/// would run in subscription order, and the loggers subscribe before the runtime starts.
/// </summary>
internal sealed class ProcessShutdown
{
    public static ProcessShutdown Default { get; } = CreateHooked();

    private readonly object sync = new();
    private readonly List<Func<Task>> steps = new();
    private readonly List<Action> finalFlushes = new();
    private int ran;

    internal ProcessShutdown()
    {
    }

    private static ProcessShutdown CreateHooked()
    {
        var shutdown = new ProcessShutdown();
        AppDomain.CurrentDomain.ProcessExit += (_, _) => shutdown.Run();
        return shutdown;
    }

    public void OnShutdown(Func<Task> step)
    {
        lock (sync)
        {
            steps.Add(step);
        }
    }

    public void OnFinalFlush(Action flush)
    {
        lock (sync)
        {
            finalFlushes.Add(flush);
        }
    }

    public void RemoveFinalFlush(Action flush)
    {
        lock (sync)
        {
            finalFlushes.Remove(flush);
        }
    }

    /// <summary>Runs once: every step (in parallel, each bounding its own time), then every final flush.</summary>
    internal void Run()
    {
        if (Interlocked.Exchange(ref ran, 1) == 1) return;

        List<Func<Task>> stepsToRun;
        List<Action> flushesToRun;
        lock (sync)
        {
            stepsToRun = steps.ToList();
            flushesToRun = finalFlushes.ToList();
        }

        // ProcessExit is synchronous: run the async steps off any synchronization context and block until they end.
        Task.Run(() => Task.WhenAll(stepsToRun.Select(RunStepAsync))).GetAwaiter().GetResult();

        foreach (var flush in flushesToRun)
        {
            try
            {
                flush();
            }
            catch
            {
                // Nothing left to report to; the next flush still runs.
            }
        }
    }

    private static async Task RunStepAsync(Func<Task> step)
    {
        try
        {
            await step();
        }
        catch
        {
            // A step logs its own failures (flushed after); one failing mustn't skip the others or the flushes.
        }
    }
}
