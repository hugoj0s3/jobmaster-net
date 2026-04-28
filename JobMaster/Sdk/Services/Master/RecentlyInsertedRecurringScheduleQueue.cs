using System.Collections.Concurrent;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services.Master;

internal class RecentlyInsertedRecurringScheduleQueue : JobMasterClusterAwareComponent, IRecentlyInsertedRecurringScheduleQueue
{
    private readonly ConcurrentQueue<Guid> queue = new();

    public RecentlyInsertedRecurringScheduleQueue(JobMasterClusterConnectionConfig clusterConnConfig) : base(clusterConnConfig)
    {
    }

    public void Enqueue(Guid scheduleId)
    {
        queue.Enqueue(scheduleId);
    }

    public IReadOnlyList<Guid> Dequeue(int maxCount)
    {
        var result = new List<Guid>(Math.Min(maxCount, queue.Count));
        while (result.Count < maxCount && queue.TryDequeue(out var id))
        {
            result.Add(id);
        }
        return result;
    }
}
