using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Services.Master;

internal interface IMasterRecurringSchedulesService : IJobMasterClusterAwareService
{
    Task UpdateAsync(RecurringScheduleRawModel scheduleRaw);
    void Update(RecurringScheduleRawModel scheduleRaw);
    Task AddAsync(RecurringScheduleRawModel scheduleRaw);
    void Add(RecurringScheduleRawModel scheduleRaw);

    /// <summary>
    /// Inserts each schedule that isn't on master yet (existing rows are left untouched) and returns every schedule's
    /// status: inserted now, already on master, or not confirmed (safe to save again). All three keys are always
    /// present. Unlike <see cref="IMasterRecurringScheduleIntakeService"/>, no status rule.
    /// </summary>
    Task<IDictionary<BulkInsertStatus, IList<Guid>>> BulkInsertIfNotExistsAsync(IList<RecurringScheduleRawModel> schedules);

    /// <summary>Returns which of <paramref name="recurringScheduleIds"/> exist on master.</summary>
    Task<IList<Guid>> QueryExistingIdsAsync(IList<Guid> recurringScheduleIds);
    void UpsertStatic(StaticRecurringScheduleDefinition definition);
    IList<RecurringScheduleRawModel> Query(RecurringScheduleQueryCriteria queryCriteria);
    Task<IList<RecurringScheduleRawModel>> QueryAsync(RecurringScheduleQueryCriteria queryCriteria);
    Task<IList<Guid>> QueryIdsAsync(RecurringScheduleQueryCriteria queryCriteria);
    Task<IList<RecurringScheduleRawModel>> AcquireAndFetchAsync(RecurringScheduleQueryCriteria queryCriteria, DateTime expiresAtUtc);
    Task<IList<RecurringScheduleRawModel>> AcquireAndFetchByIdsAsync(IList<Guid> ids, DateTime expiresAtUtc);
    long Count(RecurringScheduleQueryCriteria queryCriteria);
    Task<long> ProbeCountForAcquireAsync(RecurringScheduleQueryCriteria queryCriteria);
    RecurringScheduleRawModel? Get(Guid recurringScheduleId);
    Task<RecurringScheduleRawModel?> GetAsync(Guid recurringScheduleId);
    void BulkUpdateStaticDefinitionLastEnsured(IList<string> staticDefinitionIds, DateTime ensuredAt);
    Task<int> InactivateStaticDefinitionsOlderThanAsync(DateTime cutoff);
}