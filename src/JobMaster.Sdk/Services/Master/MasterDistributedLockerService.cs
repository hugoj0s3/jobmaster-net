using JobMaster.Contracts.Utils;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Repositories.Master;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services.Master;

public class MasterDistributedLockerService : JobMasterClusterAwareComponent, IMasterDistributedLockerService
{
    private IMasterDistributedLockerRepository masterDistributedLockerRepository = null!;

    public readonly static TimeSpan MaxDurationToLock = TimeSpan.FromHours(49); // ensure it really need to be this long.
    

    public MasterDistributedLockerService(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IMasterDistributedLockerRepository masterDistributedLockerRepository) : base(clusterConnConfig)
    {
        this.masterDistributedLockerRepository = masterDistributedLockerRepository;
    }
    
    public bool IsLocked(string key)
    {
        return masterDistributedLockerRepository.IsLocked(key);
    }
    
    public string? TryLock(string key, TimeSpan leaseDuration)
    {
        if (!JobMasterStringUtils.IsValidForId(key))
        {
            throw new ArgumentException($"Invalid key format: {key}");
        }
        
        if (leaseDuration > MaxDurationToLock)
        {
            throw new ArgumentException($"Duration to lock cannot be longer than {MaxDurationToLock}");
        }
        
        var safeLeaseDuration = leaseDuration.Add(JobMasterConstants.ClockSkewPadding);
        
        return masterDistributedLockerRepository.TryLock(key, safeLeaseDuration);
    }
    
    public bool ReleaseLock(string key, string? lockToken)
    {
        if (string.IsNullOrEmpty(lockToken))
        {
            return false;
        }

        // lockKeys.ValidateKeyFormat(key);
        return masterDistributedLockerRepository.ReleaseLock(key, lockToken!);
    }

    public bool ForceReleaseLock(string key)
    {
        return masterDistributedLockerRepository.ForceReleaseLock(key);
    }
}