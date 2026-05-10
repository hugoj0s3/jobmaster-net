using System.Linq;
using System;
using System.Threading.Tasks;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

/// <summary>
/// Information about a task for monitoring and debugging purposes
/// </summary>
internal class TaskInfo
{
    public int Index { get; set; }
    public DateTime StartTime { get; set; }
    public TimeSpan ElapsedTime { get; set; }
    public TimeSpan Timeout { get; set; }
    public bool IsTimedOut { get; set; }
    public TaskStatus TaskStatus { get; set; }
}

internal class TaskQueueControl<T> : ITaskQueueControl<T>, IDisposable
{
    private ITaskQueueItem<T>?[] Tasks { get; set; }
    
    private Queue<ITaskQueueItem<T>> WaitingQueue { get; set; } = new();
    
    private readonly HashSet<string> itemIds = new HashSet<string>();
    
    public int WaitingQueueCapacity { get; private set; }
    
    private readonly object syncLock = new();
    private readonly Func<T, bool>? preEnqueueAction;
    
    private bool isShuttingDown = false;
    

    public static TaskQueueControl<T> Create(
        JobMasterPriority priority, 
        double factor = 1, 
        Func<T, bool>? preEnqueueAction = null)
    {
        var runCapacity = priority switch
        {
            JobMasterPriority.VeryLow => 2,
            JobMasterPriority.Low => 3,
            JobMasterPriority.Medium => 4,
            JobMasterPriority.High => 5,
            JobMasterPriority.Critical => 6,
            _ => 1
        };
        
        runCapacity = (int)Math.Round(runCapacity * factor, 0);
        if (runCapacity < 1) runCapacity = 1;

        var waitingQueueCapacity = runCapacity * 5;

        return new TaskQueueControl<T>(runCapacity, waitingQueueCapacity,  preEnqueueAction);
    }
    
    private TaskQueueControl(int runCapacity, int waitingQueueCapacity, Func<T, bool>? preEnqueueAction = null)
    {
        this.preEnqueueAction = preEnqueueAction;
        Tasks = new ITaskQueueItem<T>?[runCapacity];
        WaitingQueue = new Queue<ITaskQueueItem<T>>();
        WaitingQueueCapacity = waitingQueueCapacity;
    }
    
    public bool StartQueuedTasksIfHasSlotAvailable()
    {
        lock (syncLock)
        {
            if (isShuttingDown) return false;
            
            bool hasStartedAny = false;
            for (int i = 0; i < Tasks.Length; i++)
            {
                if (Tasks[i]?.Task?.IsCompleted == true || 
                    Tasks[i]?.Task?.IsCanceled == true || 
                    Tasks[i]?.Task?.IsFaulted == true)
                {
                    // Remove ID from tracking when task completes
                    if (Tasks[i] != null)
                    {
                        itemIds.Remove(Tasks[i]!.Id);
                    }
                    Tasks[i]?.Dispose();
                    Tasks[i] = null;
                }
                
                if (Tasks[i] == null)
                {
                    if (WaitingQueue.Count > 0)
                    {
                        var queueItem = WaitingQueue.Dequeue();
                        Tasks[i] = queueItem;
                        queueItem.Start();
                        hasStartedAny = true;
                    }
                }
            }
            
            return hasStartedAny;
        }
    }
    
    public int CountRunning()
    {
        lock (syncLock)
        {
            if (isShuttingDown) return 0;
            return Tasks.Count(x => x is not null);
        }
    }
    
    public int CountAvailability()
    {
        lock (syncLock)
        {
            if (isShuttingDown) return 0;
            return WaitingQueueCapacity - WaitingQueue.Count;
        }
    }
    
    public int CountWaiting()
    {
        lock (syncLock)
        {
            if (isShuttingDown) return 0;
            return WaitingQueue.Count;
        }
    }

    public bool Contains(string id)
    {
        lock (syncLock)
        {
            return itemIds.Contains(id);
        }
    }
    
    public bool Enqueue(ITaskQueueItem<T> queueItem)
    {
        lock (syncLock)
        {
            if (isShuttingDown) return false;

            if (itemIds.Contains(queueItem.Id))
            {
                return true; // Already in queue or running, treat as success
            }

            if (WaitingQueue.Count >= WaitingQueueCapacity)
            {
                return false;
            }

            if (preEnqueueAction is not null && queueItem.Value is not null)
            {
                var result = preEnqueueAction(queueItem.Value);
                if (!result)
                {
                    return false;
                }
            }

            WaitingQueue.Enqueue(queueItem);
            itemIds.Add(queueItem.Id);
            return true;
        }
    }

    public async Task<IList<T>> ShutdownAsync()
    {
        IList<T> result = new List<T>();
        lock (syncLock)
        {
            isShuttingDown = true;
            result = WaitingQueue
                .Select(x => x.Value)
                .ToList();
            
            WaitingQueue.Clear();
            itemIds.Clear();
        } 
            
        await Task.Delay(TimeSpan.FromSeconds(1));

        ITaskQueueItem<T>[] taskSnapshot;
        lock (syncLock)
        {
            taskSnapshot = Tasks.Where(x => x is not null).Select(x => x!).ToArray();
        }

        var tasksToStop = taskSnapshot.Where(x => !x.Task.IsCompleted && !x.Task.IsCanceled && !x.Task.IsFaulted).ToList();
        if (tasksToStop.Count > 0)
        {
            var timeoutAt = DateTime.UtcNow.AddSeconds(5);
            while (tasksToStop.Any(x => !x.Task.IsCompleted && !x.Task.IsCanceled && !x.Task.IsFaulted))
            {
                if (DateTime.UtcNow >= timeoutAt)
                {
                    // Timeout reached, stop waiting for tasks to complete
                    break;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(250));
            }

            tasksToStop = tasksToStop.Where(x => !x.Task.IsCompleted && !x.Task.IsCanceled && !x.Task.IsFaulted).ToList();

            if (tasksToStop.Count > 0)
            {
                tasksToStop.ForEach(x => x?.Dispose());
            }
        }
        
        return result;
    }

    /// <summary>
    /// Returns the configured timeout for each slot that currently has a running task.
    /// Used to compute the average running timeout for postpone duration calculations.
    /// </summary>
    public IEnumerable<TimeSpan> GetRunningTimeouts()
    {
        lock (syncLock)
        {
            return Tasks
                .Where(x => x is not null)
                .Select(x => x!.Timeout)
                .ToList();
        }
    }

    public IList<string> GetIds()
    {
        lock (syncLock)
        {
            return new List<string>(itemIds);
        }
    }

    /// <summary>
    /// Gets information about currently running tasks, including their timeout status
    /// </summary>
    /// <returns>A list of task information for monitoring purposes</returns>
    public IList<TaskInfo> GetTasksInfo()
    {
        lock (syncLock)
        {
            var taskInfos = new List<TaskInfo>();
            
            for (int i = 0; i < Tasks.Length; i++)
            {
                var task = Tasks[i];
                if (task != null)
                {
                    taskInfos.Add(new TaskInfo
                    {
                        Index = i,
                        StartTime = task.EnqueuedAt,
                        ElapsedTime = task.GetElapsedTime(),
                        Timeout = task.Timeout,
                        IsTimedOut = task.IsTimedOut(),
                        TaskStatus = task.Task.Status
                    });
                }
            }
            
            return taskInfos;
        }
    }

    public void Dispose()
    {
        lock (syncLock)
        {
            foreach (var taskContainerItem in Tasks)
            {
                if (taskContainerItem is not null)
                {
                    taskContainerItem.Dispose();
                }
            }
            
            itemIds.Clear();
            WaitingQueue.Clear();
        }
    }
}