using JobMaster.Sdk.Contracts.Ioc.Markups;
using JobMaster.Sdk.Contracts.Models.GenericRecords;

namespace JobMaster.Sdk.Contracts.Repositories.Master;

public interface IMasterGenericRecordRepository : IJobMasterClusterAwareMasterRepository
{
    GenericRecordEntry? Get(string groupId, string entryId, bool includeExpired = false);
    Task<GenericRecordEntry?> GetAsync(string groupId, string entryId, bool includeExpired = false);
    
    IList<GenericRecordEntry> Query(string groupId, GenericRecordQueryCriteria? criteria = null);
    Task<IList<GenericRecordEntry>> QueryAsync(string groupId, GenericRecordQueryCriteria? criteria = null);
    
    void Upsert(GenericRecordEntry recordEntry);
    Task UpsertAsync(GenericRecordEntry recordEntry);
    
    void Insert(GenericRecordEntry recordEntry);
    Task InsertAsync(GenericRecordEntry recordEntry);
    
    void Update(GenericRecordEntry recordEntry);
    Task UpdateAsync(GenericRecordEntry recordEntry);
    
    void Delete(string groupId, string id);
    Task DeleteAsync(string groupId, string id);
    Task BulkInsertAsync(IList<GenericRecordEntry> records);
    Task<int> DeleteExpiredAsync(DateTime expiresAtTo, int limit);
    Task<int> DeleteByCreatedAtAsync(string groupId, DateTime createdAtTo, int limit);
}