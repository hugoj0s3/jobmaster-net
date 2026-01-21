using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Repositories.Master;

public interface IMasterRecurringSchedulesRepository : IJobMasterClusterAwareMasterRepository
{
    void Add(RecurringScheduleRawModel scheduleRaw);
    Task AddAsync(RecurringScheduleRawModel scheduleRaw);
    
    void Update(RecurringScheduleRawModel scheduleRaw);
    Task UpdateAsync(RecurringScheduleRawModel scheduleRaw);
    IList<RecurringScheduleRawModel> Query(RecurringScheduleQueryCriteria queryCriteria);
    Task<IList<RecurringScheduleRawModel>> QueryAsync(RecurringScheduleQueryCriteria queryCriteria);
    RecurringScheduleRawModel? Get(Guid recurringScheduleId);
    Task<RecurringScheduleRawModel?> GetAsync(Guid recurringScheduleId);
    
    RecurringScheduleRawModel? GetByStaticId(string staticId);
    
    bool BulkUpdatePartitionLockId(IList<Guid> recurringScheduleIds, int lockId, DateTime expiresAt);
    long Count(RecurringScheduleQueryCriteria queryCriteria);

    Task<int> PurgeTerminatedAsync(DateTime cutoffUtc, int limit);

    void BulkUpdateStaticDefinitionLastEnsuredByStaticIds(IList<string> staticDefinitionIds, DateTime ensuredAt);

    Task<int> InactivateStaticDefinitionsOlderThanAsync(DateTime cutoff);
}