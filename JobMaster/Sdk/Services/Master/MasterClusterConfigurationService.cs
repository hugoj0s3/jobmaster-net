using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.LocalCache;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services.Master;

public class MasterClusterConfigurationService : JobMasterClusterAwareComponent, IMasterClusterConfigurationService
{
    private IMasterGenericRecordRepository repository = null!;
    private IMasterChangesSentinelService masterChangesSentinelService = null!;
    private readonly IJobMasterLogger logger;

    private readonly IJobMasterInMemoryCache cache;
    
    private JobMasterSentinelKeys sentinelKeys = null!;
    private JobMasterInMemoryKeys cacheKeys = null!;
    
    public MasterClusterConfigurationService(
        JobMasterClusterConnectionConfig clusterConnConfig, 
        IJobMasterInMemoryCache cache,
        IMasterGenericRecordRepository repository,
        IMasterChangesSentinelService masterChangesSentinelService,
        IJobMasterLogger logger) : base(clusterConnConfig)
    {
        this.cache = cache;
        this.repository = repository;
        this.masterChangesSentinelService = masterChangesSentinelService;
        this.logger = logger;

        sentinelKeys = new JobMasterSentinelKeys(clusterConnConfig.ClusterId);
        cacheKeys = new JobMasterInMemoryKeys(ClusterConnConfig.ClusterId);
    }

    public ClusterConfigurationModel? Get()
    {
        var sentinelKey = sentinelKeys.GetMasterConfiguration();
        var cachedConfig = cache.Get<ClusterConfigurationModel>(cacheKeys.MasterConfiguration());
        
        if (cachedConfig?.Value != null &&
            !masterChangesSentinelService.HasChangesAfter(sentinelKey, cachedConfig.CreatedAt, TimeSpan.FromMinutes(5)))
        {
            return cachedConfig.Value;
        }
        
        logger.Debug($"MasterClusterConfigurationService.Get() - Configuration not cached or changed after {sentinelKey}");
        
        return GetFromRepo();
    }

    public ClusterConfigurationModel? GetNoAche()
    {
        return GetFromRepo();
    }

    private ClusterConfigurationModel? GetFromRepo()
    {
        var configurationRecord = repository.Get(MasterGenericRecordGroupIds.ClusterConfiguration, ClusterConnConfig.ClusterId);
        var configurationModel = configurationRecord?.ToObject<ClusterConfigurationModel>() ?? new ClusterConfigurationModel(ClusterConnConfig.ClusterId);
        
        // Use the cache key consistently (Get uses cacheKeys.MasterConfiguration())
        cache.Set(cacheKeys.MasterConfiguration(), configurationModel);
        return configurationModel;
    }

    public void Save(ClusterConfigurationModel clusterConfiguration)
    {
        var record = GenericRecordEntry.Create(ClusterConnConfig.ClusterId, MasterGenericRecordGroupIds.ClusterConfiguration, ClusterConnConfig.ClusterId, clusterConfiguration);
        repository.Upsert(record);
        masterChangesSentinelService.NotifyChanges(sentinelKeys.GetMasterConfiguration(), DateTime.UtcNow);
    }

    public bool IsSaved()
    {
        var configurationRecord = repository.Get(MasterGenericRecordGroupIds.ClusterConfiguration, ClusterConnConfig.ClusterId);
        return configurationRecord != null;
    }
}