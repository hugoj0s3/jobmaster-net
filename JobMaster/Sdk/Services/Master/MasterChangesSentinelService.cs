using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.LocalCache;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services.Master;

internal class MasterChangesSentinelService : JobMasterClusterAwareComponent, IMasterChangesSentinelService
{
    private IMasterGenericRecordRepository repository = null!;

    private readonly IJobMasterInMemoryCache cache;
    
    private JobMasterSentinelKeys sentinelKeys = null!;

    public MasterChangesSentinelService(
        JobMasterClusterConnectionConfig clusterConnConfig, 
        IJobMasterInMemoryCache cache,
        IMasterGenericRecordRepository repository) : base(clusterConnConfig)
    {
        this.cache = cache;
        this.repository = repository;
        sentinelKeys = new JobMasterSentinelKeys(clusterConnConfig.ClusterId);
    }
    
    public bool HasChangesAfter(string sentinelKey, DateTime lastUpdate, TimeSpan? allowedDiscrepancy = null)
    {
        if (allowedDiscrepancy.HasValue && allowedDiscrepancy.Value > JobMasterConstants.MaxAllowedDiscrepancy)
        {
            allowedDiscrepancy = JobMasterConstants.MaxAllowedDiscrepancy;
        }
        
        var cacheItem = cache.Get<DateTime?>(sentinelKey);
        if (cacheItem?.Value != null)
        {
            
            if (cacheItem.Value > lastUpdate)
                return true; // Change detected

            if (allowedDiscrepancy.HasValue && (DateTime.UtcNow - cacheItem.CreatedAt) < allowedDiscrepancy.Value)
                return false; // Cache is "fresh enough", trust it
        }

        // Cache is absent or stale, check DB
        var sentinelEntity = repository.Get(MasterGenericRecordGroupIds.Sentinel, sentinelKey)?.ToObject<SentinelRecord>();
        var lastUpdateDb = sentinelEntity?.LastUpdate;

        if (lastUpdateDb != null)
        {
            cache.Set(sentinelKey, lastUpdateDb);
        }
        else
        {
            cache.Remove(sentinelKey);
        }

        return lastUpdateDb != null && lastUpdateDb > lastUpdate;
    }

    public void NotifyChanges(string sentinelKey)
    {
        this.NotifyChanges(sentinelKey, DateTime.UtcNow);
    }
    
    public void NotifyChanges(string sentinelKey, DateTime lastUpdate)
    {
        lastUpdate = lastUpdate.ToUniversalTime();
        
        sentinelKeys.ValidateKeyFormat(sentinelKey);

        cache.Remove(sentinelKey);
        
        var entity = new SentinelRecord(ClusterConnConfig.ClusterId)
        {
            Id = sentinelKey,
            LastUpdate = lastUpdate,
        };
        
        repository.Upsert(GenericRecordEntry.Create(ClusterConnConfig.ClusterId, MasterGenericRecordGroupIds.Sentinel, sentinelKey, entity, expiresAt: DateTime.UtcNow.AddDays(5)));
    }
    
    private class SentinelRecord : JobMasterBaseModel {
        public SentinelRecord(string clusterId) : base(clusterId)
        {
        }
        
        protected SentinelRecord() {}

        public string Id { get; set; } = string.Empty;
        public DateTime LastUpdate { get; set; }
    }
}