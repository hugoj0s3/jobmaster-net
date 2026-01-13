using JobMaster.Sdk.Contracts.Ioc.Markups;

namespace JobMaster.Sdk.Contracts.Repositories.Master;

public interface IMasterDistributedLockerRepository  : IJobMasterClusterAwareMasterRepository
{
    string? TryLock(string key, TimeSpan leaseDuration);
    bool ReleaseLock(string key, string lockToken);
    bool IsLocked(string key);
    bool ForceReleaseLock(string key);
}