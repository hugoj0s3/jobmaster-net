using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using JobMaster.Sdk.Contracts.Models.Logs;

namespace JobMaster.Sdk;

public class JsonlFileLogger
{
     public string FilePath;

    private readonly List<LogItem> buffer = new();
    private readonly object sync = new();
    private readonly int maxBufferItems;
    private readonly TimeSpan flushInterval;
    private DateTime lastFlushUtc;

    private static ConcurrentDictionary<string, JsonlFileLogger> loggers = new ConcurrentDictionary<string, JsonlFileLogger>();
    
    private JsonlFileLogger(string filePath, int maxBufferItems = 500, TimeSpan? flushInterval = null)
    {
        FilePath = filePath;
        this.maxBufferItems = Math.Max(1, maxBufferItems);
        this.flushInterval = flushInterval ?? TimeSpan.FromSeconds(5);
        lastFlushUtc = DateTime.UtcNow;
    }
    
    public static void AddLogger(string clusterId, string filePath, int maxBufferItems = 500, TimeSpan? flushInterval = null)
    {
        loggers.AddOrUpdate(clusterId, _ => new JsonlFileLogger(filePath, maxBufferItems, flushInterval), (k, v) => v);
    }
    
    public static void LogMirror(LogItem logItem)
    {
        if (!loggers.TryGetValue(logItem.ClusterId, out var logger))
        {
            return;
        }
        
        logger.Log(logItem);
    }

    public void Log(LogItem logItem)
    {
        bool shouldFlush = false;
        lock (sync)
        {
            buffer.Add(logItem);
            var nowUtc = logItem.TimestampUtc.Kind == DateTimeKind.Utc
                ? logItem.TimestampUtc
                : logItem.TimestampUtc.ToUniversalTime();

            if (buffer.Count >= maxBufferItems || (nowUtc - lastFlushUtc) >= flushInterval)
            {
                lastFlushUtc = nowUtc;
                shouldFlush = true;
            }
        }

        if (!shouldFlush)
        {
            return;
        }

        List<LogItem> toWrite;
        lock (sync)
        {
            if (buffer.Count == 0) return;
            toWrite = buffer.ToList();
            buffer.Clear();
        }

        try
        {
            var groups = toWrite
                .GroupBy(i => GetChunkStartUtc(i.TimestampUtc))
                .ToDictionary(g => g.Key, g => g.ToList());

            foreach (var kv in groups)
            {
                var chunkStart = kv.Key;
                var items = kv.Value;
                var path = GetChunkFilePath(chunkStart);
                EnsureDirectory(path);

                using var stream = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read);
                using var writer = new StreamWriter(stream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
                foreach (var item in items)
                {
                    var json = JsonSerializer.Serialize(item);
                    writer.WriteLine(json);
                }
            }
        }
        catch
        {
            // Intentionally ignore IO errors to avoid impacting callers
        }
    }

    private static DateTime GetChunkStartUtc(DateTime timestamp)
    {
        var tsUtc = timestamp.Kind == DateTimeKind.Utc ? timestamp : timestamp.ToUniversalTime();
        var chunkHour = (tsUtc.Hour / 4) * 4;
        return new DateTime(tsUtc.Year, tsUtc.Month, tsUtc.Day, chunkHour, 0, 0, DateTimeKind.Utc);
    }

    private string GetChunkFilePath(DateTime chunkStartUtc)
    {
        var directory = Path.GetDirectoryName(FilePath);
        var isDirectory = string.IsNullOrWhiteSpace(Path.GetFileName(FilePath)) || Directory.Exists(FilePath);

        if (isDirectory)
        {
            var dir = Directory.Exists(FilePath) ? FilePath : (directory ?? ".");
            var name = $"logs_{chunkStartUtc:yyyyMMdd_HH}.jsonl";
            return Path.Combine(dir, name);
        }

        var baseName = Path.GetFileNameWithoutExtension(FilePath);
        var ext = ".jsonl";
        var dirName = directory ?? ".";
        var fileName = $"{baseName}_{chunkStartUtc:yyyyMMdd_HH}{ext}";
        return Path.Combine(dirName, fileName);
    }

    private static void EnsureDirectory(string fullPath)
    {
        var dir = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
        {
            Directory.CreateDirectory(dir);
        }
    }
}