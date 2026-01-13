using JobMaster.Sdk.Contracts.Ioc.Markups;

namespace JobMaster.Sdk.Contracts.Services.Master;

public interface IMasterDistributedLockerService : IJobMasterClusterAwareService
{
    bool IsLocked(string key);
    string? TryLock(string key, TimeSpan leaseDuration);
    bool ReleaseLock(string key, string? lockToken);
    bool ForceReleaseLock(string key);
}