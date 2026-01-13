using System.Diagnostics;
using System.Text;

namespace JobMaster.IntegrationTests.Utils;

public class PerformanceMonitor : IDisposable
{
    private readonly Process currentProcess;
    private readonly Timer timer;
    private readonly List<PerformanceSnapshot> snapshots = new();
    private readonly object lockObject = new();
    private bool isRunning;

    public PerformanceMonitor(TimeSpan sampleInterval)
    {
        currentProcess = Process.GetCurrentProcess();
        timer = new Timer(TakeSnapshot, null, TimeSpan.Zero, sampleInterval);
        isRunning = true;
    }

    private void TakeSnapshot(object? state)
    {
        if (!isRunning) return;

        try
        {
            currentProcess.Refresh();
            
            var snapshot = new PerformanceSnapshot
            {
                Timestamp = DateTime.UtcNow,
                CpuTimeMs = currentProcess.TotalProcessorTime.TotalMilliseconds,
                WorkingSetMB = currentProcess.WorkingSet64 / (1024.0 * 1024.0),
                PrivateMemoryMB = currentProcess.PrivateMemorySize64 / (1024.0 * 1024.0),
                ThreadCount = currentProcess.Threads.Count,
                HandleCount = currentProcess.HandleCount
            };

            lock (lockObject)
            {
                snapshots.Add(snapshot);
            }
        }
        catch
        {
            // Ignore errors during sampling
        }
    }

    public PerformanceReport GetReport()
    {
        lock (lockObject)
        {
            if (snapshots.Count == 0)
                return new PerformanceReport();

            var report = new PerformanceReport
            {
                SampleCount = snapshots.Count,
                StartTime = snapshots.First().Timestamp,
                EndTime = snapshots.Last().Timestamp,
                
                // CPU (calculate usage from deltas)
                TotalCpuTimeMs = snapshots.Last().CpuTimeMs - snapshots.First().CpuTimeMs,
                
                // Memory
                MinWorkingSetMB = snapshots.Min(s => s.WorkingSetMB),
                MaxWorkingSetMB = snapshots.Max(s => s.WorkingSetMB),
                AvgWorkingSetMB = snapshots.Average(s => s.WorkingSetMB),
                
                MinPrivateMemoryMB = snapshots.Min(s => s.PrivateMemoryMB),
                MaxPrivateMemoryMB = snapshots.Max(s => s.PrivateMemoryMB),
                AvgPrivateMemoryMB = snapshots.Average(s => s.PrivateMemoryMB),
                
                // Threads
                MinThreadCount = snapshots.Min(s => s.ThreadCount),
                MaxThreadCount = snapshots.Max(s => s.ThreadCount),
                AvgThreadCount = snapshots.Average(s => s.ThreadCount),
                
                // Handles
                MinHandleCount = snapshots.Min(s => s.HandleCount),
                MaxHandleCount = snapshots.Max(s => s.HandleCount),
                AvgHandleCount = snapshots.Average(s => s.HandleCount)
            };

            var duration = (report.EndTime - report.StartTime).TotalSeconds;
            if (duration > 0)
            {
                // CPU usage percentage (total CPU time / wall clock time / core count)
                var coreCount = Environment.ProcessorCount;
                report.AvgCpuUsagePercent = (report.TotalCpuTimeMs / 1000.0) / duration / coreCount * 100.0;
            }

            return report;
        }
    }

    public string GetReportString()
    {
        var report = GetReport();
        var sb = new StringBuilder();
        
        sb.AppendLine("==== Performance Report ====");
        sb.AppendLine($"Duration: {(report.EndTime - report.StartTime).TotalSeconds:F2}s");
        sb.AppendLine($"Samples: {report.SampleCount}");
        sb.AppendLine();
        
        sb.AppendLine("Overall Metrics:");
        sb.AppendLine($"  CPU Usage: {report.AvgCpuUsagePercent:F2}% (avg across {Environment.ProcessorCount} cores)");
        sb.AppendLine($"  Total CPU Time: {report.TotalCpuTimeMs / 1000.0:F2}s");
        sb.AppendLine($"  Working Set (MB): Min={report.MinWorkingSetMB:F2}, Max={report.MaxWorkingSetMB:F2}, Avg={report.AvgWorkingSetMB:F2}");
        sb.AppendLine($"  Private Memory (MB): Min={report.MinPrivateMemoryMB:F2}, Max={report.MaxPrivateMemoryMB:F2}, Avg={report.AvgPrivateMemoryMB:F2}");
        sb.AppendLine($"  Threads: Min={report.MinThreadCount}, Max={report.MaxThreadCount}, Avg={report.AvgThreadCount:F1}");
        sb.AppendLine($"  Handles: Min={report.MinHandleCount}, Max={report.MaxHandleCount}, Avg={report.AvgHandleCount:F1}");
        sb.AppendLine();
        
        // Time-based chunks (2 minutes each)
        var chunks = GetTimeBasedChunks(TimeSpan.FromMinutes(2));
        if (chunks.Count > 0)
        {
            sb.AppendLine("Time-Based Analysis (2-minute intervals):");
            sb.AppendLine("Time Range          | CPU%  | Mem(MB) | Threads | Handles");
            sb.AppendLine("--------------------+-------+---------+---------+--------");
            
            foreach (var chunk in chunks)
            {
                var timeRange = $"{chunk.StartOffset:mm\\:ss}-{chunk.EndOffset:mm\\:ss}";
                sb.AppendLine($"{timeRange,-19} | {chunk.AvgCpuPercent,5:F1} | {chunk.AvgMemoryMB,7:F1} | {chunk.AvgThreads,7:F1} | {chunk.AvgHandles,7:F1}");
            }
        }
        
        sb.AppendLine("============================");
        
        return sb.ToString();
    }

    private List<TimeChunk> GetTimeBasedChunks(TimeSpan chunkDuration)
    {
        lock (lockObject)
        {
            if (snapshots.Count == 0)
                return new List<TimeChunk>();

            var chunks = new List<TimeChunk>();
            var startTime = snapshots.First().Timestamp;
            var endTime = snapshots.Last().Timestamp;
            var totalDuration = endTime - startTime;
            
            var currentChunkStart = TimeSpan.Zero;
            
            while (currentChunkStart < totalDuration)
            {
                var currentChunkEnd = currentChunkStart + chunkDuration;
                if (currentChunkEnd > totalDuration)
                    currentChunkEnd = totalDuration;
                
                var chunkSnapshots = snapshots
                    .Where(s => (s.Timestamp - startTime) >= currentChunkStart && (s.Timestamp - startTime) < currentChunkEnd)
                    .ToList();
                
                if (chunkSnapshots.Count > 0)
                {
                    var cpuDelta = chunkSnapshots.Last().CpuTimeMs - chunkSnapshots.First().CpuTimeMs;
                    var timeDelta = (chunkSnapshots.Last().Timestamp - chunkSnapshots.First().Timestamp).TotalSeconds;
                    var cpuPercent = timeDelta > 0 ? (cpuDelta / 1000.0) / timeDelta / Environment.ProcessorCount * 100.0 : 0;
                    
                    chunks.Add(new TimeChunk
                    {
                        StartOffset = currentChunkStart,
                        EndOffset = currentChunkEnd,
                        AvgCpuPercent = cpuPercent,
                        AvgMemoryMB = chunkSnapshots.Average(s => s.WorkingSetMB),
                        AvgThreads = chunkSnapshots.Average(s => s.ThreadCount),
                        AvgHandles = chunkSnapshots.Average(s => s.HandleCount)
                    });
                }
                
                currentChunkStart = currentChunkEnd;
            }
            
            return chunks;
        }
    }

    public void Dispose()
    {
        isRunning = false;
        timer?.Dispose();
    }
}

public class PerformanceSnapshot
{
    public DateTime Timestamp { get; set; }
    public double CpuTimeMs { get; set; }
    public double WorkingSetMB { get; set; }
    public double PrivateMemoryMB { get; set; }
    public int ThreadCount { get; set; }
    public int HandleCount { get; set; }
}

public class TimeChunk
{
    public TimeSpan StartOffset { get; set; }
    public TimeSpan EndOffset { get; set; }
    public double AvgCpuPercent { get; set; }
    public double AvgMemoryMB { get; set; }
    public double AvgThreads { get; set; }
    public double AvgHandles { get; set; }
}

public class PerformanceReport
{
    public int SampleCount { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    
    public double TotalCpuTimeMs { get; set; }
    public double AvgCpuUsagePercent { get; set; }
    
    public double MinWorkingSetMB { get; set; }
    public double MaxWorkingSetMB { get; set; }
    public double AvgWorkingSetMB { get; set; }
    
    public double MinPrivateMemoryMB { get; set; }
    public double MaxPrivateMemoryMB { get; set; }
    public double AvgPrivateMemoryMB { get; set; }
    
    public int MinThreadCount { get; set; }
    public int MaxThreadCount { get; set; }
    public double AvgThreadCount { get; set; }
    
    public int MinHandleCount { get; set; }
    public int MaxHandleCount { get; set; }
    public double AvgHandleCount { get; set; }
}
