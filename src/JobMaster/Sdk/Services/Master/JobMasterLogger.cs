using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Services.Master;

/// <summary>
/// Cluster-aware batched logger that buffers log writes and flushes to the master repository in bulk.
/// </summary>
internal sealed class JobMasterLogger : JobMasterClusterAwareComponent, IJobMasterLogger, IDisposable
{
    private readonly IMasterLogsRepository repo;

    // Safety cap: If DB is down, don't consume more than ~10k logs in RAM.
    private const int MaxQueueLimit = 10_000;

    private const int MaxBatchSize = 100;
    private static readonly TimeSpan FlushInterval = TimeSpan.FromSeconds(15);

    // Upper bound for draining the queue on shutdown, so a down database can't hang process exit.
    private static readonly TimeSpan FinalFlushTimeout = TimeSpan.FromSeconds(5);

    private readonly ConcurrentQueue<LogItem> queue = new();
    private int queuedCount = 0;

    private readonly Timer timer;
    private readonly SemaphoreSlim flushLock = new(1, 1);
    private volatile bool disposed;
    private int disposeStarted;


    private Action<LogItem>? MirrorLog { get; }

    public JobMasterLogger(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IMasterLogsRepository repo) : base(clusterConnConfig)
    {
        this.repo = repo ?? throw new ArgumentNullException(nameof(repo));

        // Optimization: Use static delegate to avoid closure allocation
        this.timer = new Timer(static state => _ = ((JobMasterLogger)state!).SafeFlushAsync(), this, FlushInterval, FlushInterval);

        MirrorLog = clusterConnConfig.MirrorLog;

        // Nothing disposes this logger on host shutdown (the per-cluster containers are registered as instances,
        // which DI never disposes), so flush what's buffered when the process exits normally (SIGTERM, Ctrl+C,
        // host stop). A hard kill or a crash can't be helped.
        AppDomain.CurrentDomain.ProcessExit += OnProcessExit;
    }

    private void OnProcessExit(object? sender, EventArgs e)
    {
        Dispose();
    }

    public void Log(
        JobMasterLogLevel level,
        string message,
        JobMasterLogCategory? category = null,
        string? referenceId = null,
        Exception? exception = null,
        string? sourceMember = null,
        string? sourceFile = null,
        int? sourceLine = null)
    {

        var item = new LogItem
        {
            ClusterId = this.ClusterConnConfig.ClusterId,
            Level = level,
            Message = message ?? string.Empty,
            Category = category,
            ReferenceId = referenceId,
            TimestampUtc = DateTime.UtcNow,
            Host = Environment.MachineName,
            SourceMember = sourceMember,
            SourceFile = sourceFile,
            SourceLine = sourceLine,
            Id = JobMasterRandomUtil.NewGuid7(),
        };

        if (exception != null)
        {
            // Captures type, message, stack trace, and inner exceptions
            item.Message += $"{Environment.NewLine}Exception:{Environment.NewLine}{exception}";
        }

        var callback = MirrorLog;
        callback?.Invoke(item);

        if (disposed) return;

        // 1. PROTECTION: If queue is full, drop the log to save app memory.
        if (queuedCount >= MaxQueueLimit)
        {
            return;
        }

        if (level == JobMasterLogLevel.Debug)
        {
            return;
        }

        queue.Enqueue(item);
        var count = Interlocked.Increment(ref queuedCount);

        if (count >= MaxBatchSize)
        {
            // Fire-and-forget flush
            _ = SafeFlushAsync();
        }
    }

    public Task<List<LogItem>> QueryAsync(LogItemQueryCriteria criteria)
    {
        return repo.QueryAsync(criteria);
    }

    public Task<int> CountAsync(LogItemQueryCriteria criteria)
    {
        return repo.CountAsync(criteria);
    }

    public Task<LogItem?> GetAsync(Guid id)
    {
        return repo.GetAsync(id);
    }

    public void Dispose()
    {
        // Can be reached twice: an explicit Dispose and ProcessExit.
        if (Interlocked.Exchange(ref disposeStarted, 1) == 1) return;

        AppDomain.CurrentDomain.ProcessExit -= OnProcessExit;

        // 1. Stop the timer first to prevent new flush triggers
        try { timer.Dispose(); } catch { }

        // 2. FORCE Final Flush
        // We bypass SafeFlushAsync to avoid the 'disposed' check and allow a wait time.
        try
        {
            // Wait up to 2 seconds for the lock (in case a flush is currently running)
            if (flushLock.Wait(2000))
            {
                try
                {
                    // Drain everything, not one batch: up to MaxQueueLimit can be buffered. A failed insert still
                    // dequeues its batch, so this always ends; the timeout only covers a slow database.
                    // Run synchronously (Sync-over-Async) because we are in Dispose.
                    var deadline = DateTime.UtcNow.Add(FinalFlushTimeout);
                    while (queuedCount > 0 && DateTime.UtcNow < deadline)
                    {
                        FlushCoreAsync().GetAwaiter().GetResult();
                    }
                }
                finally
                {
                    flushLock.Release();
                }
            }
            else
            {
                var msg = "[JM-LOGGER] Dispose timeout. Could not acquire lock for final flush.";
                Trace.TraceError(msg);
            }
        }
        catch (Exception e)
        {
            var msg = $"[JM-LOGGER] Dispose timeout. Could not acquire lock for final flush. {e.StackTrace}";
            Trace.TraceError(msg);
        }

        // 3. NOW set disposed to true
        disposed = true;

        flushLock.Dispose();
    }

    private async Task SafeFlushAsync()
    {
        if (disposed) return;

        // Use timeout of 0 to avoid stacking tasks if lock is held during normal operations
        if (!await flushLock.WaitAsync(0).ConfigureAwait(false))
            return;

        try
        {
            await FlushCoreAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            var msg = $"[JM-LOGGER] Flush failed: {ex.GetType().Name}: {ex.Message}";
            Trace.TraceError(msg);
        }
        finally
        {
            if (!disposed)
            {
                flushLock.Release();
            }
        }
    }

    private async Task FlushCoreAsync()
    {
        if (queuedCount == 0) return;

        var limit = MaxBatchSize;
        var list = new List<LogItem>(limit);

        while (list.Count < limit && queue.TryDequeue(out var item))
        {
            Interlocked.Decrement(ref queuedCount);
            list.Add(item);
        }

        if (list.Count > 0)
        {
            try
            {
                await repo.BulkInsertAsync(list).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                var msg = $"[JM-LOGGER] Bulk insert failed: {ex.GetType().Name}: {ex.Message} (batchSize={list.Count})";
                Trace.TraceError(msg);
            }
        }
    }
}
