using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Services.Master;

internal interface IMasterRecurringSchedulesService : IJobMasterClusterAwareService
{
    Task UpdateAsync(RecurringScheduleRawModel scheduleRaw);
    void Update(RecurringScheduleRawModel scheduleRaw);
    Task AddAsync(RecurringScheduleRawModel scheduleRaw);
    void Add(RecurringScheduleRawModel scheduleRaw);
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