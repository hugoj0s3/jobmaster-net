using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.LocalCache;
using JobMaster.Sdk.Contracts.Models;
using JobMaster.Sdk.Contracts.Models.GenericRecords;
using JobMaster.Sdk.Contracts.Repositories.Master;
using JobMaster.Sdk.Contracts.Services.Master;
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