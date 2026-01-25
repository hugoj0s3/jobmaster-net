using JobMaster.Sdk.Abstractions.Ioc.Markups;

namespace JobMaster.Sdk.Abstractions.Services.Master;

internal interface IMasterDistributedLockerService : IJobMasterClusterAwareService
{
    bool IsLocked(string key);
    string? TryLock(string key, TimeSpan leaseDuration);
    bool ReleaseLock(string key, string? lockToken);
    bool ForceReleaseLock(string key);
}