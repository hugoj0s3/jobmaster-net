using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Services.Master;

internal interface IMasterRecurringSchedulesService : IJobMasterClusterAwareService
{
    Task UpsertAsync(RecurringScheduleRawModel scheduleRaw);
    void Upsert(RecurringScheduleRawModel scheduleRaw);
    void UpsertStatic(StaticRecurringScheduleDefinition definition);
    IList<RecurringScheduleRawModel> Query(RecurringScheduleQueryCriteria queryCriteria);
    Task<IList<RecurringScheduleRawModel>> QueryAsync(RecurringScheduleQueryCriteria queryCriteria);
    Task<IList<Guid>> QueryIdsAsync(RecurringScheduleQueryCriteria queryCriteria);
    Task<IList<RecurringScheduleRawModel>> AcquireAndFetchAsync(RecurringScheduleQueryCriteria queryCriteria, int partitionLockId, DateTime expiresAtUtc);
    Task<IList<RecurringScheduleRawModel>> AcquireAndFetchByIdsAsync(IList<Guid> ids, int partitionLockId, DateTime expiresAtUtc);
    long Count(RecurringScheduleQueryCriteria queryCriteria);
    RecurringScheduleRawModel? Get(Guid recurringScheduleId);
    Task<RecurringScheduleRawModel?> GetAsync(Guid recurringScheduleId);
    void BulkUpdateStaticDefinitionLastEnsured(IList<string> staticDefinitionIds, DateTime ensuredAt);
    Task<int> InactivateStaticDefinitionsOlderThanAsync(DateTime cutoff);
}