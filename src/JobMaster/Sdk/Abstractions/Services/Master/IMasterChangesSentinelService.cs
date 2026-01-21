using System.ComponentModel;
using JobMaster.Sdk.Abstractions.Ioc.Markups;

namespace JobMaster.Sdk.Abstractions.Services.Master;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IMasterChangesSentinelService : IJobMasterClusterAwareService
{
    bool HasChangesAfter(string sentinelKey, DateTime lastUpdate, TimeSpan? allowedDiscrepancy = null);
    
    void NotifyChanges(string sentinelKey, DateTime lastUpdate);
    
    void NotifyChanges(string sentinelKey);
}