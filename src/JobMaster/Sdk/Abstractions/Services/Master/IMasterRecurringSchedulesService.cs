using System.ComponentModel;
using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Services.Master;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IMasterRecurringSchedulesService : IJobMasterClusterAwareService
{
    Task UpsertAsync(RecurringScheduleRawModel scheduleRaw);
    void Upsert(RecurringScheduleRawModel scheduleRaw);
    void UpsertStatic(StaticRecurringScheduleDefinition definition);
    IList<RecurringScheduleRawModel> Query(RecurringScheduleQueryCriteria queryCriteria);
    Task<IList<RecurringScheduleRawModel>> QueryAsync(RecurringScheduleQueryCriteria queryCriteria);
    Task<IList<Guid>> QueryIdsAsync(RecurringScheduleQueryCriteria queryCriteria);
    long Count(RecurringScheduleQueryCriteria queryCriteria);
    RecurringScheduleRawModel? Get(Guid recurringScheduleId);
    Task<RecurringScheduleRawModel?> GetAsync(Guid recurringScheduleId);
    bool BulkUpdatePartitionLockId(IList<Guid> recurringScheduleIds, int lockId, DateTime expiresAt);
    void BulkUpdateStaticDefinitionLastEnsured(IList<string> staticDefinitionIds, DateTime ensuredAt);
    Task<int> InactivateStaticDefinitionsOlderThanAsync(DateTime cutoff);
}