using JobMaster.Sdk.Contracts.Ioc.Markups;

namespace JobMaster.Sdk.Contracts.Services.Master;

public interface IMasterChangesSentinelService : IJobMasterClusterAwareService
{
    bool HasChangesAfter(string sentinelKey, DateTime lastUpdate, TimeSpan? allowedDiscrepancy = null);
    
    void NotifyChanges(string sentinelKey, DateTime lastUpdate);
    
    void NotifyChanges(string sentinelKey);
}