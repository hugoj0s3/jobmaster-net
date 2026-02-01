using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services.Master;

/// <summary>
/// Cluster-aware batched logger that buffers log writes and flushes to the master repository in bulk.
/// </summary>
internal sealed class JobMasterLogger : JobMasterClusterAwareComponent, IJobMasterLogger, IDisposable
{
    private readonly IMasterGenericRecordRepository repo;

    // Safety cap: If DB is down, don't consume more than ~10k logs in RAM.
    private const int MaxQueueLimit = 10_000;

    private const int MaxBatchSize = 100;
    private static readonly TimeSpan FlushInterval = TimeSpan.FromSeconds(15);

    private readonly ConcurrentQueue<LogItem> queue = new();
    private int queuedCount = 0;

    private readonly Timer timer;
    private readonly SemaphoreSlim flushLock = new(1, 1);
    private volatile bool disposed;
    
    
    private Action<LogItem>? MirrorLog { get; }

    public JobMasterLogger(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IMasterGenericRecordRepository repo) : base(clusterConnConfig)
    {
        this.repo = repo ?? throw new ArgumentNullException(nameof(repo));
        
        // Optimization: Use static delegate to avoid closure allocation
        this.timer = new Timer(static state => _ = ((JobMasterLogger)state!).SafeFlushAsync(), this, FlushInterval, FlushInterval);
        
        MirrorLog = clusterConnConfig.MirrorLog;
    }

    public void Log(
        JobMasterLogLevel level,
        string message,
        JobMasterLogSubjectType? subjectType = null,
        string? subjectId = null,
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
            SubjectType = subjectType,
            SubjectId = subjectId,
            TimestampUtc = DateTime.UtcNow,
            Host = Environment.MachineName,
            SourceMember = sourceMember,
            SourceFile = sourceFile,
            SourceLine = sourceLine,
            Id = Guid.NewGuid(),
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
            // Optional: Log to console that we are dropping logs due to backpressure
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

    public async Task<List<LogItem>> QueryAsync(LogItemQueryCriteria criteria)
    {
        var genericRecordQueryCriteria = ToGenericRecordQueryCriteria(criteria);

        var genericRecords = await repo.QueryAsync(MasterGenericRecordGroupIds.Log, genericRecordQueryCriteria);
        return genericRecords.Select(x => ToLogItem(x)).ToList();
    }
    
    public Task<int> CountAsync(LogItemQueryCriteria criteria)
    {
        var genericRecordQueryCriteria = ToGenericRecordQueryCriteria(criteria);
        return repo.CountAsync(MasterGenericRecordGroupIds.Log, genericRecordQueryCriteria);
    }

    public async Task<LogItem?> GetAsync(Guid id)
    {
        var record = await repo.GetAsync(MasterGenericRecordGroupIds.Log, id.ToString("N"));
        if (record == null) return null;
        
        return ToLogItem(record);
    }

    public void Dispose()
    {
        if (disposed) return;
        
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
                    // Run synchronously (Sync-over-Async) because we are in Dispose
                    FlushCoreAsync().GetAwaiter().GetResult(); 
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

    private static LogItem ToLogItem(GenericRecordEntry x)
    {
        JobMasterLogSubjectType? subjectType;
        if (!string.IsNullOrEmpty(x.SubjectType))
        {
            if (Enum.TryParse<JobMasterLogSubjectType>(x.SubjectType, ignoreCase: true, out var subjectTypeParsed))
            {
                subjectType = subjectTypeParsed;
            }
            else
            {
                subjectType = null;
            }
        }
        else
        {
            subjectType = null;
        }
        var logItem = x.ToObject<LogItem>();
        logItem.SubjectId = x.SubjectId;
        logItem.SubjectType = subjectType;
        logItem.ClusterId = x.ClusterId;
        logItem.Id = Guid.Parse(x.EntryId);
            
        return logItem;
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
                // Optimistic: Try bulk insert
                var listEntries = list.Select(item => ToEntry(item)).ToList();
                await repo.BulkInsertAsync(listEntries).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                var bulkMsg = $"[JM-LOGGER] Bulk insert failed: {ex.GetType().Name}: {ex.Message} (batchSize={list.Count})";
                Trace.TraceError(bulkMsg);

                // Fallback: Try individual inserts
                foreach (var logItem in list)
                {
                    try
                    {
                        await repo.InsertAsync(ToEntry(logItem)).ConfigureAwait(false);
                    }
                    catch (Exception insertEx)
                    {
                        var msg = $"[JM-EMERGENCY-LOG] DB Fail: {insertEx.Message} | Log: {logItem}";
                        Trace.TraceError(msg);
                    }
                }
            }
        }
    }

    private GenericRecordEntry ToEntry(LogItem item)
    {
        var payload = new LogPayload
        {
            Level = (int)item.Level,
            Message = $"{item.Message}",
            TimestampUtc = item.TimestampUtc,
            Host = item.Host,
            SourceMember = item.SourceMember,
            SourceFile = item.SourceFile,
            SourceLine = item.SourceLine
        };

        if (string.IsNullOrEmpty(item.SubjectId))
        {
            return GenericRecordEntry.Create(
                ClusterConnConfig.ClusterId,
                MasterGenericRecordGroupIds.Log,
                item.Id,
                payload);
        }
        
        return GenericRecordEntry.Create(
            ClusterConnConfig.ClusterId,
            MasterGenericRecordGroupIds.Log,
            item.Id,
            item.SubjectType?.ToString() ?? string.Empty,
            item.SubjectId!,
            payload);
    }
    
    private static GenericRecordQueryCriteria ToGenericRecordQueryCriteria(LogItemQueryCriteria criteria)
    {
        var genericRecordQueryCriteria = new GenericRecordQueryCriteria()
        {
            Filters = new List<GenericRecordValueFilter>(),
            OrderBy = GenericRecordQueryOrderByTypeId.CreatedAtDesc,
            ReadIsolationLevel = ReadIsolationLevel.FastSync,
            Offset = criteria.Offset,
            Limit = criteria.CountLimit,
        };
        var filters = genericRecordQueryCriteria.Filters;
        
        if (criteria.FromTimestamp.HasValue)
        {
            filters.Add(new GenericRecordValueFilter()
            {
                Key = nameof(LogPayload.TimestampUtc),
                Operation = GenericFilterOperation.Gte,
                Value = criteria.FromTimestamp.Value
            });
        }

        if (criteria.ToTimestamp.HasValue)
        {
            filters.Add(new GenericRecordValueFilter()
            {
                Key = nameof(LogPayload.TimestampUtc),
                Operation = GenericFilterOperation.Lte,
                Value = criteria.ToTimestamp.Value
            });
        }

        if (criteria.Level.HasValue)
        {
            filters.Add(new GenericRecordValueFilter()
            {
                Key = nameof(LogPayload.Level),
                Operation = GenericFilterOperation.Eq,
                Value = (int)criteria.Level.Value
            });
        }
        
        if (!string.IsNullOrEmpty(criteria.Keyword))
        {
            filters.Add(new GenericRecordValueFilter()
            {
                Key = nameof(LogPayload.Message),
                Operation = GenericFilterOperation.Contains,
                Value = criteria.Keyword
            });
        }

        if (criteria.SubjectType.HasValue)
        {
            genericRecordQueryCriteria.SubjectType = criteria.SubjectType.Value.ToString();
        }

        if (!string.IsNullOrEmpty(criteria.SubjectId))
        {
            genericRecordQueryCriteria.SubjectIds = new List<string>() { criteria.SubjectId! };
        }

        return genericRecordQueryCriteria;
    }
}