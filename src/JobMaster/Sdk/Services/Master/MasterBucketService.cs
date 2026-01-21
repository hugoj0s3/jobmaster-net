using System;
using System.Collections.Concurrent;
using JobMaster.Abstractions.Models;
using JobMaster.Internals;
using JobMaster.Sdk.Abstractions.BucketSelector;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.LocalCache;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services.Master;

public class MasterBucketsService : JobMasterClusterAwareComponent, IMasterBucketsService
{

    private IMasterGenericRecordRepository masterGenericRecordRepository = null!;
    private IMasterChangesSentinelService masterChangesSentinelService = null!;
    private IMasterDistributedLockerService masterDistributedLockerService = null!;
    private IMasterAgentWorkersService masterAgentWorkersService = null!;
    private IAgentJobsDispatcherService masterAgentsDispatcherService = null!;
    private IMasterClusterConfigurationService masterClusterConfigurationService = null!;
    private IJobMasterLogger logger = null!;

    private readonly IJobMasterInMemoryCache jobMasterMemoryCache;
    private JobMasterInMemoryKeys cacheKeys = null!;
    private JobMasterSentinelKeys sentinelKeys = null!;
    private readonly IBucketSelectorAlgorithm bucketSelectorAlgorithm;
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


        cacheKeys = new JobMasterInMemoryKeys(clusterConnConfig.ClusterId);
        sentinelKeys = new JobMasterSentinelKeys(clusterConnConfig.ClusterId);
        lockKeys = new JobMasterLockKeys(clusterConnConfig.ClusterId);
    }

    public async Task DestroyAsync(string bucketId)
    {
        var bucket = this.masterGenericRecordRepository.Get(MasterGenericRecordGroupIds.Bucket, bucketId);
        if (bucket is null)
            return;

        var bucketModel = bucket.ToObject<BucketModel>();
        
        if (bucketModel.Status != BucketStatus.ReadyToDelete)
        {
            logger.Error("Bucket is not ready to delete", JobMasterLogSubjectType.Bucket, bucketId);
            return;
        }

        if (await this.masterAgentsDispatcherService.HasJobsAsync(bucketModel.AgentConnectionId, bucketId))
        {
            logger.Error("Bucket has jobs", JobMasterLogSubjectType.Bucket, bucketId);
            return;
        }
        
        await masterGenericRecordRepository.DeleteAsync(MasterGenericRecordGroupIds.Bucket, bucketId);
        await this.masterAgentsDispatcherService.DestroyBucketAsync(bucketModel.AgentConnectionId, bucketId);
        
        this.masterChangesSentinelService.NotifyChanges(sentinelKeys.Bucket(bucketId));
    }

    public async Task<BucketModel> CreateAsync(AgentConnectionId agentConnectionId, string workerId, JobMasterPriority priority)
    {
        var worker = await this.masterAgentWorkersService.GetWorkerAsync(workerId);
        var agentConfiguration = ClusterConnConfig.GetAgentConnectionConfig(agentConnectionId.IdValue);

        // In Insert method
        ValidateAgentAndWorker(agentConnectionId, workerId, worker, agentConfiguration);

        var bucketModel = CreateAndValidateBucketRecord(agentConnectionId, workerId, priority, worker!.Name, worker?.WorkerLane, agentConfiguration.RepositoryTypeId);

        var genericRecord = GenericRecordEntry.Create(ClusterConnConfig.ClusterId, MasterGenericRecordGroupIds.Bucket, bucketModel.Id, bucketModel);
        await masterGenericRecordRepository.InsertAsync(genericRecord);
        await this.masterAgentsDispatcherService.CreateBucketAsync(agentConnectionId, bucketModel.Id);

        this.masterChangesSentinelService.NotifyChanges(sentinelKeys.BucketsAvailableForJobs());
        this.masterChangesSentinelService.NotifyChanges(sentinelKeys.Bucket(bucketModel.Id));

        return bucketModel;
    }

    public void Update(BucketModel model)
    {
        var bucket = this.masterGenericRecordRepository.Get(MasterGenericRecordGroupIds.Bucket, model.Id);
        if (bucket is null)
            return;
        
        var genericRecord = GenericRecordEntry.Create(
            ClusterConnConfig.ClusterId, 
            MasterGenericRecordGroupIds.Bucket,
            model.Id, 
            model);
        masterGenericRecordRepository.Update(genericRecord);

        this.masterChangesSentinelService.NotifyChanges(sentinelKeys.BucketsAvailableForJobs());
        this.masterChangesSentinelService.NotifyChanges(sentinelKeys.Bucket(model.Id));
    }

    public async Task UpdateAsync(BucketModel model)
    {
        var bucketGenericRecord =
            await this.masterGenericRecordRepository.GetAsync(MasterGenericRecordGroupIds.Bucket, model.Id);
        if (bucketGenericRecord is null)
            return;

        var genericRecord = GenericRecordEntry.Create(
            ClusterConnConfig.ClusterId, 
            MasterGenericRecordGroupIds.Bucket,
            model.Id, 
            model);
        await masterGenericRecordRepository.UpdateAsync(genericRecord);
        
        this.masterChangesSentinelService.NotifyChanges(sentinelKeys.BucketsAvailableForJobs());
        this.masterChangesSentinelService.NotifyChanges(sentinelKeys.Bucket(model.Id));
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
        var cacheKey = cacheKeys.Bucket(id);
        var sentinelKey = sentinelKeys.Bucket(id);
        var bucketFromCache = jobMasterMemoryCache.Get<BucketModel>(cacheKey);
        if (bucketFromCache?.Value is not null &&
            !masterChangesSentinelService.HasChangesAfter(sentinelKey, bucketFromCache.Value.CreatedAt, allowedDiscrepancy))
        {
            return bucketFromCache?.Value;
        }

        var bucket = masterGenericRecordRepository.Get(MasterGenericRecordGroupIds.Bucket, id)?.ToObject<BucketModel>();
        if (bucket != null)
        {
            jobMasterMemoryCache.Set(cacheKey, bucket);
        }

        return bucket;
    }

    public async Task<IList<BucketModel>> QueryAllNoCacheAsync(BucketStatus? bucketStatus = null)
    {
        var criteria = new GenericRecordQueryCriteria();

        if (bucketStatus.HasValue)
        {
            criteria.Filters.Add(new GenericRecordValueFilter()
            {
                Key = "Status",
                Operation = GenericFilterOperation.Eq,
                Value = (int)bucketStatus.Value,
            });
        }

        var records = await masterGenericRecordRepository.QueryAsync(MasterGenericRecordGroupIds.Bucket, criteria);
        return records.Select(x => x.ToObject<BucketModel>()).ToList();
    }

    public List<BucketModel> QueryAllNoCache(BucketStatus? bucketStatus = null)
    {
        var criteria = new GenericRecordQueryCriteria();

        if (bucketStatus.HasValue)
        {
            criteria.Filters.Add(new GenericRecordValueFilter()
            {
                Key = "Status",
                Operation = GenericFilterOperation.Eq,
                Value = (int)bucketStatus.Value,
            });
        }

        var records = masterGenericRecordRepository.Query(MasterGenericRecordGroupIds.Bucket, criteria);
        return Enumerable.Select(records, x => x.ToObject<BucketModel>()).ToList();
    }
    
    public const string AnyWorkerLaneKeyword = "[Any]";
    private BucketModel? SelectBucketForJob(TimeSpan? allowedDiscrepancy, JobMasterPriority? jobPriority = null, string? workerLane = null)
    {
        var availableBuckets = GetBucketsAvailableFromCache(allowedDiscrepancy);
        if (availableBuckets is null)
        {
            var criteria = new GenericRecordQueryCriteria()
            {
                Filters = new List<GenericRecordValueFilter>()
                {
                    new()
                    {
                        Key = nameof(BucketModel.Status),
                        Operation = GenericFilterOperation.Eq,
                        Value = (int)BucketStatus.Active,
                    }
                }
            };

            var records = this.masterGenericRecordRepository.Query(MasterGenericRecordGroupIds.Bucket, criteria);
            
            availableBuckets = records.Select(x => x.ToObject<BucketModel>()).ToList();
            availableBuckets = availableBuckets.Where(x => !string.IsNullOrEmpty(x.AgentWorkerId)).ToList();

            jobMasterMemoryCache.Set(cacheKeys.BucketsAvailableForJobs(), availableBuckets);
        }

        var applicableBuckets = FilterApplicableBuckets(availableBuckets, jobPriority, workerLane);
        return bucketSelectorAlgorithm.Select(applicableBuckets);
    }


    private IList<BucketModel> FilterApplicableBuckets(
        IList<BucketModel> availableBuckets,
        JobMasterPriority? jobPriority,
        string? workerLane)
    {
        return availableBuckets
            .Where(b => !jobPriority.HasValue || b.Priority == jobPriority)
            .Where(b => string.Equals(workerLane, AnyWorkerLaneKeyword, StringComparison.InvariantCultureIgnoreCase) || 
                        string.Equals(b.WorkerLane ?? string.Empty, workerLane ?? string.Empty, StringComparison.OrdinalIgnoreCase))
            .Where(b => ClusterConnConfig.TryGetAgentConnectionConfig(b.AgentConnectionId.IdValue) != null)
            .OrderByDescending(x => x.CreatedAt)
            .ToList();
    }

    private List<BucketModel>? GetBucketsAvailableFromCache(TimeSpan? allowedDiscrepancy)
    {
        var cacheKey = cacheKeys.BucketsAvailableForJobs();
        var sentinelKey = sentinelKeys.BucketsAvailableForJobs();

        var lastAvailableBucketQueriedResult =
            jobMasterMemoryCache.Get<List<BucketModel>>(cacheKey);
        if (lastAvailableBucketQueriedResult?.Value is null)
        {
            return null;
        }

        var lastQueriedAt = lastAvailableBucketQueriedResult.CreatedAt;
        var hasChangesAfter = this.masterChangesSentinelService.HasChangesAfter(sentinelKey, lastQueriedAt, allowedDiscrepancy: allowedDiscrepancy);

        if (!hasChangesAfter)
        {
            return lastAvailableBucketQueriedResult.Value;
        }

        return null;
    }

    private BucketModel CreateAndValidateBucketRecord(AgentConnectionId agentConnectionId, string workerId, JobMasterPriority priority,
        string workerName, string? workerLane, string repositoryTypeId)
    {
        var bucketModel = new BucketModel(ClusterConnConfig.ClusterId)
        {
            AgentConnectionId = agentConnectionId,
            AgentWorkerId = workerId,
            Priority = priority,
            Status = BucketStatus.Active,
            CreatedAt = DateTime.UtcNow,
            Color = JobMasterRandomUtil.GetEnum<BucketColor>(),
            WorkerLane = workerLane,
            RepositoryTypeId = repositoryTypeId,
        };

        var workerLaneSegment = string.IsNullOrWhiteSpace(workerLane) ? string.Empty : $":{workerLane}";
        bucketModel.Name = $"{workerName}{workerLaneSegment}:Bucket-{JobMasterIdUtil.NewNanoId()}";
        bucketModel.Id = $"{bucketModel.Name}:{JobMasterIdUtil.NewShortId()}";
        
        if (!JobMasterStringUtils.IsValidForId(bucketModel.Id))
            throw new ArgumentException(
                $"Invalid bucket ID format. Only letters, numbers, underscore (_), hyphen (-), and dot (.) are allowed. Received: '{bucketModel.Id}'",
                nameof(bucketModel.Id));

        return bucketModel;
    }

    private void ValidateAgentAndWorker(AgentConnectionId agentConnectionId, string workerId, object? worker, object? agentConfiguration)
    {
        if (worker is null)
            throw new ArgumentException($"Worker with id {workerId} not found.", nameof(workerId));
        if (agentConfiguration is null)
            throw new ArgumentException($"Agent configuration with id {agentConnectionId} not found.", nameof(agentConnectionId));
    }
}