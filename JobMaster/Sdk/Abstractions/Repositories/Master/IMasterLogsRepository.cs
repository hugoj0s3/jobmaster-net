using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Logs;

namespace JobMaster.Sdk.Abstractions.Repositories.Master;

internal interface IMasterLogsRepository : IJobMasterClusterAwareMasterRepository
{
    Task BulkInsertAsync(IList<LogItem> items);
    Task<List<LogItem>> QueryAsync(LogItemQueryCriteria criteria);
    Task<int> CountAsync(LogItemQueryCriteria criteria);
    Task<LogItem?> GetAsync(Guid id);

    /// <summary>
    /// Deletes log entries older than <paramref name="timestampTo"/>. When <paramref name="excludeCategory"/>
    /// is set, entries in that category are skipped entirely — used to exclude JobMasterLogCategory.JobExecution,
    /// whose lifecycle is owned by the job-archiving/purge runner instead, so it doesn't race this blanket purge.
    /// </summary>
    Task<int> DeleteByTimestampAsync(DateTime timestampTo, int limit, JobMasterLogCategory? excludeCategory = null);

    /// <summary>Returns all LogItems in the given category whose ReferenceId is one of the given values.</summary>
    Task<IList<LogItem>> QueryForReferenceIdsAsync(JobMasterLogCategory category, IList<string> referenceIds);

    /// <summary>Deletes the given log entries by id.</summary>
    Task<int> DeleteByIdsAsync(IList<Guid> ids);
}
