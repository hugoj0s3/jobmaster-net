using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.LocalCache;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Cache;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services.Master;

internal class MasterClusterConfigurationService : JobMasterClusterAwareComponent, IMasterClusterConfigurationService
{
    private IMasterGenericRecordRepository repository = null!;
    private IMasterChangesSentinelService masterChangesSentinelService = null!;
    private readonly IJobMasterLogger logger;

    private readonly IJobMasterInMemoryCache cache;
    private readonly RetryDeadlockPolicy retryDeadlockPolicy;
    private readonly SentinelCachedReader sentinelCachedReader;

    private JobMasterSentinelKeys sentinelKeys = null!;
    private JobMasterInMemoryKeys cacheKeys = null!;

    public MasterClusterConfigurationService(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IJobMasterInMemoryCache cache,
        IMasterGenericRecordRepository repository,
        IMasterChangesSentinelService masterChangesSentinelService,
        IKnownExceptionIdentifier knownExceptionIdentifier,
        IJobMasterLogger logger) : base(clusterConnConfig)
    {
        this.cache = cache;
        this.repository = repository;
        this.masterChangesSentinelService = masterChangesSentinelService;
        this.logger = logger;

        sentinelKeys = new JobMasterSentinelKeys(clusterConnConfig.ClusterId);
        cacheKeys = new JobMasterInMemoryKeys(ClusterConnConfig.ClusterId);

        this.retryDeadlockPolicy =
            new RetryDeadlockPolicy(knownExceptionIdentifier, TimeSpan.FromMilliseconds(250), 3);

        this.sentinelCachedReader =
            new SentinelCachedReader(cache, masterChangesSentinelService);
    }

    public ClusterConfigurationModel? Get()
    {
        return retryDeadlockPolicy.Exec(() => sentinelCachedReader.GetOrFetch(
            cacheKey: cacheKeys.MasterConfiguration(),
            sentinelKey: sentinelKeys.GetMasterConfiguration(),
            factory: FetchFromRepo,
            allowedDiscrepancy: JobMasterConstants.ClusterConfigurationAllowDiscrepancy,
            valueFactoryLockDuration: TimeSpan.FromSeconds(2),
            durationToExpire: JobMasterConstants.DefaultCacheEntryExpiry));
    }

    public ClusterConfigurationModel? GetFresh()
    {
        // Bypass the sentinel check but still warm the cache so subsequent Get() calls
        // don't double-fetch.
        var configurationModel = retryDeadlockPolicy.Exec(FetchFromRepo);
        cache.Set(cacheKeys.MasterConfiguration(), configurationModel);
        return configurationModel;
    }

    private ClusterConfigurationModel FetchFromRepo()
    {
        var configurationRecord = repository.Get(MasterGenericRecordGroupIds.ClusterConfiguration, ClusterConnConfig.ClusterId);
        return configurationRecord?.ToObject<ClusterConfigurationModel>()
               ?? new ClusterConfigurationModel(ClusterConnConfig.ClusterId);
    }

    public void Save(ClusterConfigurationModel clusterConfiguration)
    {
        masterChangesSentinelService.NotifyChanges(sentinelKeys.GetMasterConfiguration(), DateTime.UtcNow);
        var record = GenericRecordEntry.Create(ClusterConnConfig.ClusterId, MasterGenericRecordGroupIds.ClusterConfiguration, ClusterConnConfig.ClusterId, clusterConfiguration);
        repository.Upsert(record);
    }

    public bool IsSaved()
    {
        var configurationRecord = repository.Get(MasterGenericRecordGroupIds.ClusterConfiguration, ClusterConnConfig.ClusterId);
        return configurationRecord != null;
    }
}
