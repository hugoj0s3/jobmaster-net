using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Logs;

namespace JobMaster.Sdk.Abstractions.Repositories.Master;

internal interface IMasterLogsRepository : IJobMasterClusterAwareMasterRepository
{
    Task BulkInsertAsync(IList<LogItem> items);
    Task<List<LogItem>> QueryAsync(LogItemQueryCriteria criteria);
    Task<int> CountAsync(LogItemQueryCriteria criteria);
    Task<LogItem?> GetAsync(Guid id);
    Task<int> DeleteByTimestampAsync(DateTime timestampTo, int limit);
}
