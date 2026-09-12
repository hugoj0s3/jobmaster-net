using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Repositories.Master;

internal interface IMasterRecurringSchedulesRepository : IJobMasterClusterAwareMasterRepository
{
    void Add(RecurringScheduleRawModel scheduleRaw);
    Task AddAsync(RecurringScheduleRawModel scheduleRaw);

    void Update(RecurringScheduleRawModel scheduleRaw);
    Task UpdateAsync(RecurringScheduleRawModel scheduleRaw);

    bool Exists(Guid recurringScheduleId);
    Task<bool> ExistsAsync(Guid recurringScheduleId);
    IList<RecurringScheduleRawModel> Query(RecurringScheduleQueryCriteria queryCriteria);
    Task<IList<RecurringScheduleRawModel>> QueryAsync(RecurringScheduleQueryCriteria queryCriteria);
    RecurringScheduleRawModel? Get(Guid recurringScheduleId);
    Task<RecurringScheduleRawModel?> GetAsync(Guid recurringScheduleId);
    
    RecurringScheduleRawModel? GetByStaticId(string staticId);

    Task<IList<RecurringScheduleRawModel>> AcquireAndFetchAsync(RecurringScheduleQueryCriteria queryCriteria, Guid partitionLockId, DateTime expiresAtUtc);
    
    long Count(RecurringScheduleQueryCriteria queryCriteria);

    /// <summary>
    /// Returns the count of acquirable (not actively locked) recurring schedules matching the criteria.
    /// MetadataFilters are not supported and will throw.
    /// </summary>
    Task<long> ProbeCountForAcquireAsync(RecurringScheduleQueryCriteria queryCriteria);

    Task<int> PurgeTerminatedAsync(DateTime cutoffUtc, int limit);

    void BulkUpdateStaticDefinitionLastEnsuredByStaticIds(IList<string> staticDefinitionIds, DateTime ensuredAt);

    Task<int> InactivateStaticDefinitionsOlderThanAsync(DateTime cutoff);

    /// <summary>
    /// Selects terminated recurring schedules eligible for purging (same candidate criteria as
    /// <see cref="PurgeTerminatedAsync"/>) as full rows, without deleting them. Used by the
    /// archive-then-delete flow.
    /// </summary>
    Task<IList<RecurringScheduleRawModel>> QueryTerminatedToPurgeAsync(DateTime cutoffUtc, int limit);

    /// <summary>
    /// Deletes the given recurring schedules (plus their metadata). Shared by
    /// <see cref="PurgeTerminatedAsync"/> and the archive-then-delete flow.
    /// </summary>
    Task<int> DeleteByIdsAsync(IList<Guid> ids);

    /// <summary>
    /// Inserts each recurring schedule that doesn't already exist (by cluster id + id) in this
    /// repository's cluster. Existing rows are left untouched. Every schedule must be in a final status.
    /// </summary>
    Task BulkInsertIfNotExistsAsync(IList<RecurringScheduleRawModel> schedules);
}