using JobMaster.Sdk.Abstractions.Ioc.Markups;

namespace JobMaster.Sdk.Abstractions.Services.Master;

internal interface IRecentlyInsertedRecurringScheduleQueue : IJobMasterClusterAwareService
{
    void Enqueue(Guid scheduleId);
    IReadOnlyList<Guid> Dequeue(int maxCount);
}
