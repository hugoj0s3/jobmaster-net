using System;
using System.Collections.Concurrent;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.BucketSelector;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Cache;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.LocalCache;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Services.Master;

internal class MasterBucketsService : JobMasterClusterAwareComponent, IMasterBucketsService
{
    private IMasterGenericRecordRepository masterGenericRecordRepository = null!;
    private IMasterChangesSentinelService masterChangesSentinelService = null!;
    private IMasterDistributedLockerService masterDistributedLockerService = null!;
    private IMasterAgentWorkersService masterAgentWorkersService = null!;
    private IAgentJobsDispatcherService masterAgentsDispatcherService = null!;
    private IMasterClusterConfigurationService masterClusterConfigurationService = null!;

    private readonly IJobMasterInMemoryCache jobMasterMemoryCache;
    private readonly IBucketSelectorAlgorithm bucketSelectorAlgorithm;
    private readonly SentinelCachedReader sentinelCachedReader;
    private readonly RetryDeadlockPolicy retryDeadlockPolicy;
    private readonly IJobMasterLogger logger;
    private static readonly ConcurrentDictionary<string, int> Seq = new();

    private JobMasterInMemoryKeys cacheKeys = null!;
    private JobMasterSentinelKeys sentinelKeys = null!;
    private JobMasterLockKeys lockKeys = null!;

    public MasterBucketsService(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IBucketSelectorAlgorithm bucketSelectorAlgorithm,
        IJobMasterInMemoryCache jobMasterMemoryCache,
        IMasterGenericRecordRepository masterGenericRecordRepository,
        IMasterChangesSentinelService masterChangesSentinelService,
        IMasterDistributedLockerService masterDistributedLockerService,
        IMasterAgentWorkersService masterAgentWorkersService,
        IAgentJobsDispatcherService masterAgentsDispatcherService,
        IMasterClusterConfigurationService masterClusterConfigurationService,
        IKnownExceptionIdentifier knownExceptionIdentifier,
        IJobMasterLogger logger) : base(clusterConnConfig)
    {
        this.bucketSelectorAlgorithm = bucketSelectorAlgorithm;
        this.jobMasterMemoryCache = jobMasterMemoryCache;
        this.masterGenericRecordRepository = masterGenericRecordRepository;
        this.masterChangesSentinelService = masterChangesSentinelService;
        this.masterDistributedLockerService = masterDistributedLockerService;
        this.masterAgentWorkersService = masterAgentWorkersService;
        this.masterAgentsDispatcherService = masterAgentsDispatcherService;
        this.masterClusterConfigurationService = masterClusterConfigurationService;
        this.logger = logger;

        cacheKeys = new JobMasterInMemoryKeys(clusterConnConfig.ClusterId);
        sentinelKeys = new JobMasterSentinelKeys(clusterConnConfig.ClusterId);
        lockKeys = new JobMasterLockKeys(clusterConnConfig.ClusterId);

        this.retryDeadlockPolicy = new RetryDeadlockPolicy(knownExceptionIdentifier, TimeSpan.FromMilliseconds(250), 3);
        this.sentinelCachedReader = new SentinelCachedReader(jobMasterMemoryCache, masterChangesSentinelService);
    }

    public async Task DestroyAsync(string bucketId)
    {
        var bucket = await this.masterGenericRecordRepository.GetAsync(MasterGenericRecordGroupIds.Bucket, bucketId);
        if (bucket is null)
            return;

        var bucketModel = bucket.ToObject<BucketModel>();

        if (bucketModel.Status != BucketStatus.ReadyToDelete)
        {
            logger.Error("Bucket is not ready to delete", JobMasterLogCategory.Bucket, bucketId);
            return;
        }

        if (await this.masterAgentsDispatcherService.HasJobsAsync(bucketModel.AgentConnectionId, bucketId))
        {
            logger.Error("Bucket has jobs", JobMasterLogCategory.Bucket, bucketId);
            return;
        }

        this.masterChangesSentinelService.NotifyChanges(sentinelKeys.AllBuckets());
        this.masterChangesSentinelService.NotifyChanges(sentinelKeys.Bucket(bucketId));

        await masterGenericRecordRepository.DeleteAsync(MasterGenericRecordGroupIds.Bucket, bucketId);
        await this.masterAgentsDispatcherService.DestroyBucketAsync(bucketModel.AgentConnectionId, bucketId);
    }

    public async Task<BucketModel> CreateAsync(
        AgentConnectionId agentConnectionId,
        string workerId,
        JobMasterPriority priority,
        BucketType type = BucketType.Standard)
    {
        var worker = await this.masterAgentWorkersService.GetWorkerAsync(workerId);
        var agentConfiguration = ClusterConnConfig.GetAgentConnectionConfig(agentConnectionId.IdValue);

        ValidateAgentAndWorker(agentConnectionId, workerId, worker, agentConfiguration);

        var bucketModel = CreateAndValidateBucketRecord(
            agentConnectionId,
            worker!.HostId,
            workerId,
            priority,
            worker!.Name,
            worker?.WorkerLane,
            agentConfiguration.RepositoryTypeId,
            type);

        this.masterChangesSentinelService.NotifyChanges(sentinelKeys.AllBuckets());
        this.masterChangesSentinelService.NotifyChanges(sentinelKeys.Bucket(bucketModel.Id));

        var genericRecord = GenericRecordEntry.Create(ClusterConnConfig.ClusterId, MasterGenericRecordGroupIds.Bucket, bucketModel.Id, bucketModel);
        await masterGenericRecordRepository.InsertAsync(genericRecord);
        await this.masterAgentsDispatcherService.CreateBucketAsync(agentConnectionId, bucketModel.Id);

        return bucketModel;
    }

    public void Update(BucketModel model)
    {
        var bucket = this.masterGenericRecordRepository.Get(MasterGenericRecordGroupIds.Bucket, model.Id);
        if (bucket is null)
            return;

        this.masterChangesSentinelService.NotifyChanges(sentinelKeys.AllBuckets());
        this.masterChangesSentinelService.NotifyChanges(sentinelKeys.Bucket(model.Id));

        var genericRecord = GenericRecordEntry.Create(
            ClusterConnConfig.ClusterId,
            MasterGenericRecordGroupIds.Bucket,
            model.Id,
            model);
        masterGenericRecordRepository.Upsert(genericRecord);
    }

    public async Task UpdateAsync(BucketModel model)
    {
        var bucketGenericRecord =
            await this.masterGenericRecordRepository.GetAsync(MasterGenericRecordGroupIds.Bucket, model.Id);
        if (bucketGenericRecord is null)
            return;

        this.masterChangesSentinelService.NotifyChanges(sentinelKeys.AllBuckets());
        this.masterChangesSentinelService.NotifyChanges(sentinelKeys.Bucket(model.Id));

        var genericRecord = GenericRecordEntry.Create(
            ClusterConnConfig.ClusterId,
            MasterGenericRecordGroupIds.Bucket,
            model.Id,
            model);
        await masterGenericRecordRepository.UpsertAsync(genericRecord);
    }

    public BucketModel? SelectBucket(TimeSpan? allowedDiscrepancy, JobMasterPriority? jobPriority = null, string? workerLane = null)
    {
        return SelectBucketForJob(allowedDiscrepancy, jobPriority, workerLane);
    }

    public Task<BucketModel?> SelectBucketAsync(TimeSpan? allowedDiscrepancy, JobMasterPriority? jobPriority = null, string? workerLane = null)
    {
        return Task.FromResult(SelectBucketForJob(allowedDiscrepancy, jobPriority, workerLane));
    }

    public BucketModel? Get(string id, TimeSpan? allowedDiscrepancy)
    {
        return retryDeadlockPolicy.Exec(() => sentinelCachedReader.GetOrFetch(
            cacheKey: cacheKeys.Bucket(id),
            sentinelKey: sentinelKeys.Bucket(id),
            factory: () => masterGenericRecordRepository.Get(MasterGenericRecordGroupIds.Bucket, id)?.ToObject<BucketModel>(),
            allowedDiscrepancy: allowedDiscrepancy,
            valueFactoryLockDuration: TimeSpan.FromSeconds(2),
            durationToExpire: JobMasterConstants.DefaultCacheEntryExpiry));
    }

    public async Task<IList<BucketModel>> QueryAllNoCacheAsync(BucketStatus? bucketStatus = null)
    {
        return await QueryAsync(new MasterBucketQueryCriteria()
        {
            Status = bucketStatus,
            ReadIsolationLevel = ReadIsolationLevel.Consistent,
        });
    }

    public IList<BucketModel> QueryAllNoCache(BucketStatus? bucketStatus = null)
    {
        return Query(new MasterBucketQueryCriteria()
        {
            Status = bucketStatus,
            ReadIsolationLevel = ReadIsolationLevel.Consistent,
        });
    }

    public async Task<IList<BucketModel>> QueryAsync(MasterBucketQueryCriteria criteria, TimeSpan? allowedDiscrepancy = null)
    {
        if (allowedDiscrepancy.HasValue)
        {
            var all = await Task.FromResult(GetAllBucketsFromCache(allowedDiscrepancy.Value));
            return ApplyCriteria(all, criteria);
        }

        var records = await masterGenericRecordRepository
            .QueryAsync(MasterGenericRecordGroupIds.Bucket, ToGenericRecordQueryCriteria(criteria));
        return records.Select(x => x.ToObject<BucketModel>()).ToList();
    }

    public IList<BucketModel> Query(MasterBucketQueryCriteria criteria, TimeSpan? allowedDiscrepancy = null)
    {
        if (allowedDiscrepancy.HasValue)
        {
            var all = GetAllBucketsFromCache(allowedDiscrepancy.Value);
            return ApplyCriteria(all, criteria);
        }

        var records = masterGenericRecordRepository.Query(MasterGenericRecordGroupIds.Bucket, ToGenericRecordQueryCriteria(criteria));
        return records.Select(x => x.ToObject<BucketModel>()).ToList();
    }

    public async Task<int> CountAsync(MasterBucketQueryCriteria criteria)
    {
        return await masterGenericRecordRepository.CountAsync(MasterGenericRecordGroupIds.Bucket, ToGenericRecordQueryCriteria(criteria));
    }

    public int Count(MasterBucketQueryCriteria criteria)
    {
        return masterGenericRecordRepository.Count(MasterGenericRecordGroupIds.Bucket, ToGenericRecordQueryCriteria(criteria));
    }

    private BucketModel? SelectBucketForJob(TimeSpan? allowedDiscrepancy, JobMasterPriority? jobPriority = null, string? workerLane = null)
    {
        var allBuckets = GetAllBucketsFromCache(allowedDiscrepancy ?? JobMasterConstants.BucketFastAllowDiscrepancy);

        var availableBuckets = allBuckets
            .Where(x => x.Status == BucketStatus.Active)
            .Where(x => x.BucketType != BucketType.Fallback)
            .Where(x => !string.IsNullOrEmpty(x.AgentWorkerId))
            .Where(x => !string.IsNullOrEmpty(x.AgentConnectionId?.IdValue))
            .ToList();

        var applicableBuckets = FilterApplicableBuckets(availableBuckets, jobPriority, workerLane);
        return bucketSelectorAlgorithm.Select(applicableBuckets);
    }

    private List<BucketModel> GetAllBucketsFromCache(TimeSpan allowedDiscrepancy)
    {
        return retryDeadlockPolicy.Exec(() => sentinelCachedReader.GetOrFetch(
            cacheKey: cacheKeys.AllBuckets(),
            sentinelKey: sentinelKeys.AllBuckets(),
            factory: () =>
            {
                var records = masterGenericRecordRepository.Query(MasterGenericRecordGroupIds.Bucket, new GenericRecordQueryCriteria());
                return records.Select(x => x.ToObject<BucketModel>()).Where(x => x != null).ToList()!;
            },
            allowedDiscrepancy: allowedDiscrepancy,
            valueFactoryLockDuration: TimeSpan.FromSeconds(2),
            durationToExpire: JobMasterConstants.DefaultCacheEntryExpiry));
    }

    private static IList<BucketModel> ApplyCriteria(IList<BucketModel> buckets, MasterBucketQueryCriteria criteria)
    {
        var query = buckets.AsEnumerable();

        if (criteria.Status.HasValue)
            query = query.Where(b => b.Status == criteria.Status.Value);

        if (criteria.Statuses.Count > 0)
            query = query.Where(b => criteria.Statuses.Contains(b.Status));

        if (!string.IsNullOrEmpty(criteria.AgentConnectionId))
            query = query.Where(b => b.AgentConnectionId.IdValue == criteria.AgentConnectionId);

        if (criteria.Priority.HasValue)
            query = query.Where(b => b.Priority == criteria.Priority.Value);

        if (!string.IsNullOrEmpty(criteria.AgentWorkerId))
            query = query.Where(b => b.AgentWorkerId == criteria.AgentWorkerId);

        if (!string.IsNullOrEmpty(criteria.WorkerLane))
            query = query.Where(b => b.WorkerLane == criteria.WorkerLane);

        return query.ToList();
    }

    private IList<BucketModel> FilterApplicableBuckets(
        IList<BucketModel> availableBuckets,
        JobMasterPriority? jobPriority,
        string? workerLane)
    {
        return availableBuckets
            .Where(b => !string.IsNullOrWhiteSpace(b?.AgentConnectionId?.IdValue))
            .Where(b => !jobPriority.HasValue || b.Priority == jobPriority)
            .Where(b => string.Equals(b.WorkerLane ?? string.Empty, workerLane ?? string.Empty, StringComparison.OrdinalIgnoreCase))
            .Where(b => ClusterConnConfig.TryGetAgentConnectionConfig(b.AgentConnectionId.IdValue) != null)
            .OrderByDescending(x => x.CreatedAt)
            .ToList();
    }

    private BucketModel CreateAndValidateBucketRecord(
        AgentConnectionId agentConnectionId,
        HostId hostId,
        string workerId,
        JobMasterPriority priority,
        string workerName,
        string? workerLane,
        string repositoryTypeId,
        BucketType type)
    {
        var bucketModel = new BucketModel(ClusterConnConfig.ClusterId)
        {
            AgentConnectionId = agentConnectionId,
            AgentWorkerId = workerId,
            HostId = hostId,
            Priority = priority,
            Status = BucketStatus.Active,
            CreatedAt = DateTime.UtcNow,
            Color = JobMasterRandomUtil.GetEnum<BucketColor>(),
            WorkerLane = workerLane,
            RepositoryTypeId = repositoryTypeId,
            BucketType = type,
        };

        if (type == BucketType.Fallback)
            bucketModel.Name = $"{workerName}:fallback:bucket-";
        else
            bucketModel.Name = $"{workerName}:{priority}:bucket-";

        var seq = Seq.AddOrUpdate(bucketModel.Name, 0, (key, oldVal) => oldVal + 1);
        bucketModel.Name += seq;

        bucketModel.Id = $"{bucketModel.Name}:{JobMasterIdGenUtil.NewShortId()}";

        if (!JobMasterStringUtils.IsValidForId(bucketModel.Id))
        {
            throw new ArgumentException(
                $"Invalid bucket ID format. Only letters, numbers, underscore (_), hyphen (-), and dot (.) are allowed. Received: '{bucketModel.Id}'",
                nameof(bucketModel.Id));
        }

        return bucketModel;
    }

    private void ValidateAgentAndWorker(AgentConnectionId agentConnectionId, string workerId, object? worker, object? agentConfiguration)
    {
        if (worker is null)
            throw new ArgumentException($"Worker with id {workerId} not found.", nameof(workerId));
        if (agentConfiguration is null)
            throw new ArgumentException($"Agent configuration with id {agentConnectionId} not found.", nameof(agentConnectionId));
    }

    private GenericRecordQueryCriteria ToGenericRecordQueryCriteria(MasterBucketQueryCriteria criteria)
    {
        var genericRecordQueryCriteria = new GenericRecordQueryCriteria();

        if (criteria.Status.HasValue)
        {
            genericRecordQueryCriteria.Filters.Add(new()
            {
                Key = nameof(BucketModel.Status),
                Operation = GenericFilterOperation.Eq,
                Value = (int)criteria.Status.Value,
            });
        }

        if (!string.IsNullOrEmpty(criteria.AgentConnectionId))
        {
            genericRecordQueryCriteria.Filters.Add(new()
            {
                Key = nameof(BucketModel.AgentConnectionId),
                Operation = GenericFilterOperation.Eq,
                Value = criteria.AgentConnectionId,
            });
        }

        if (!string.IsNullOrEmpty(criteria.AgentWorkerId))
        {
            genericRecordQueryCriteria.Filters.Add(new()
            {
                Key = nameof(BucketModel.AgentWorkerId),
                Operation = GenericFilterOperation.Eq,
                Value = criteria.AgentWorkerId,
            });
        }

        if (criteria.Priority.HasValue)
        {
            genericRecordQueryCriteria.Filters.Add(new()
            {
                Key = nameof(BucketModel.Priority),
                Operation = GenericFilterOperation.Eq,
                Value = (int)criteria.Priority,
            });
        }

        if (!string.IsNullOrEmpty(criteria.WorkerLane))
        {
            genericRecordQueryCriteria.Filters.Add(new()
            {
                Key = nameof(BucketModel.WorkerLane),
                Operation = GenericFilterOperation.Eq,
                Value = criteria.WorkerLane,
            });
        }

        genericRecordQueryCriteria.ReadIsolationLevel = criteria.ReadIsolationLevel;

        return genericRecordQueryCriteria;
    }
}
