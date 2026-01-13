using System.Text.Json;
using JobMaster.Contracts.Serialization;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Models;
using JobMaster.Sdk.Contracts.Models.GenericRecords;
using JobMaster.Sdk.Contracts.Models.Jobs;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Models.RecurringSchedules;
using JobMaster.Sdk.Contracts.Repositories.Agent;
using JobMaster.Sdk.Contracts.Serialization;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Repositories;

public abstract class AgentJobsDispatcherRepository<TSavePending, TProcessing> : JobMasterClusterAwareComponent, IAgentJobsDispatcherRepository<TSavePending, TProcessing>
    where TSavePending : class, IAgentRawMessagesDispatcherRepository
    where TProcessing : class, IAgentRawMessagesDispatcherRepository
{
    private readonly IMasterClusterConfigurationService masterClusterConfigurationService;
    private readonly TSavePending savePendingRepository;
    private readonly TProcessing processingRepository;
    private readonly IJobMasterLogger logger;

    protected JobMasterAgentConnectionConfig AgentConnConfig = null!;
 

    protected AgentJobsDispatcherRepository(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IMasterClusterConfigurationService masterClusterConfigurationService,
        TSavePending savePendingRepository,
        TProcessing processingRepository,
        IJobMasterLogger logger) : base(clusterConnConfig)
    {
        this.masterClusterConfigurationService = masterClusterConfigurationService;
        this.savePendingRepository = savePendingRepository ?? throw new ArgumentNullException(nameof(savePendingRepository));
        this.processingRepository = processingRepository ?? throw new ArgumentNullException(nameof(processingRepository));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }
    
    public abstract string AgentRepoTypeId { get; }

    public void Initialize(JobMasterAgentConnectionConfig agentConnConfig)
    {
        AgentConnConfig = agentConnConfig;
        savePendingRepository.Initialize(agentConnConfig);
        processingRepository.Initialize(agentConnConfig);
    }
    
    public string PushToSaving(RecurringScheduleRawModel recurringScheduleRaw)
    {
        var fullBucketAddressId = FullBucketAddressIdsUtil.GetRecurringScheduleSavePendingBucketAddress(recurringScheduleRaw.BucketId);
        var json = InternalJobMasterSerializer.Serialize(recurringScheduleRaw);
        var correlationId = GetCorrelationId(recurringScheduleRaw);

        EnforceDispatchSizeLimit(json, correlationId);

        return savePendingRepository.PushMessage(fullBucketAddressId, json, recurringScheduleRaw.CreatedAt, correlationId);
    }
    
    public async Task<string> PushToSavingAsync(RecurringScheduleRawModel recurringScheduleRaw)
    {
        var fullBucketAddressId = FullBucketAddressIdsUtil.GetRecurringScheduleSavePendingBucketAddress(recurringScheduleRaw.BucketId);
        var json = InternalJobMasterSerializer.Serialize(recurringScheduleRaw);
        var correlationId = GetCorrelationId(recurringScheduleRaw);

        EnforceDispatchSizeLimit(json, correlationId);

        return await savePendingRepository.PushMessageAsync(fullBucketAddressId, json, recurringScheduleRaw.CreatedAt, correlationId);
    }
    
    public string PushSavePendingJob(JobRawModel jobRaw)
    {
        var fullBucketAddressId = FullBucketAddressIdsUtil.GetJobSavePendingBucketAddress(jobRaw.BucketId);
        var json = InternalJobMasterSerializer.Serialize(jobRaw);
        var correlationId = GetCorrelationId(jobRaw);

        EnforceDispatchSizeLimit(json, correlationId);

        return savePendingRepository.PushMessage(fullBucketAddressId, json, jobRaw.CreatedAt, correlationId);
    }
    
    public async Task<string> PushSavePendingJobAsync(JobRawModel jobRaw)
    {
        var fullBucketAddressId = FullBucketAddressIdsUtil.GetJobSavePendingBucketAddress(jobRaw.BucketId);
        var json = InternalJobMasterSerializer.Serialize(jobRaw);
        var correlationId = GetCorrelationId(jobRaw);

        EnforceDispatchSizeLimit(json, correlationId);

        return await savePendingRepository.PushMessageAsync(fullBucketAddressId, json, jobRaw.CreatedAt, correlationId);
    }
    
    public string PushToProcessing(JobRawModel jobRaw)
    {
        var fullBucketAddressId = FullBucketAddressIdsUtil.GetJobProcessingBucketAddress(jobRaw.BucketId);
        var json = InternalJobMasterSerializer.Serialize(jobRaw);
        var correlationId = GetCorrelationId(jobRaw);

        EnforceDispatchSizeLimit(json, correlationId);

        return processingRepository.PushMessage(fullBucketAddressId, json, jobRaw.ScheduledAt, correlationId);
    }
    
    public async Task<string> PushToProcessingAsync(JobRawModel jobRaw)
    {
        var fullBucketAddressId = FullBucketAddressIdsUtil.GetJobProcessingBucketAddress(jobRaw.BucketId);
        var json = InternalJobMasterSerializer.Serialize(jobRaw);
        var correlationId = GetCorrelationId(jobRaw);

        EnforceDispatchSizeLimit(json, correlationId);

        return await processingRepository.PushMessageAsync(fullBucketAddressId, json, jobRaw.ScheduledAt, correlationId);
    }

    public async Task<IList<string>> BulkPushSavePendingJobAsync(string bucketId, IList<JobRawModel> jobRaw)
    {
        var fullBucketAddressId = FullBucketAddressIdsUtil.GetJobSavePendingBucketAddress(bucketId);
        IList<(string payload, DateTime referenceTime, string correlationId)> messages = new List<(string payload, DateTime referenceTime, string correlationId)>();
        foreach (var job in jobRaw)
        {
            if (job.BucketId != bucketId)
            {
                throw new ArgumentException("All jobs must be in the same bucket");
            }

            var json = InternalJobMasterSerializer.Serialize(job);
            var correlationId = GetCorrelationId(job);

            EnforceDispatchSizeLimit(json, correlationId);

            messages.Add((json, job.CreatedAt, correlationId));
        }

        return await savePendingRepository.BulkPushMessageAsync(fullBucketAddressId, messages);
    }
    
    public async Task<IList<RecurringScheduleRawModel>> DequeueSavePendingRecurAsync(string bucketId, int numberOfJobs)
    {
        if (IsAutoDequeueForSaving)
        {
            return new List<RecurringScheduleRawModel>();
        }

        var fullBucketAddressId = FullBucketAddressIdsUtil.GetRecurringScheduleSavePendingBucketAddress(bucketId);
        var messages = await savePendingRepository.DequeueMessagesAsync(fullBucketAddressId, numberOfJobs);

        var results = new List<RecurringScheduleRawModel>();
        foreach (var message in messages)
        {
            try
            {
                var recurringSchedule = InternalJobMasterSerializer.Deserialize<RecurringScheduleRawModel>(message.Payload);
                results.Add(recurringSchedule);
            }
            catch (JsonException e)
            {
                logger.Critical($"Failed to deserialize recurring schedule. Payload: {message.Payload}", JobMasterLogSubjectType.Bucket, bucketId, exception: e);
            }
        }

        return results;
    }

    /// <summary>
    /// Dequeues jobs from the save pending queue
    /// </summary>
    /// <param name="bucketId">The bucket ID</param>
    /// <param name="numberOfJobs">Number of jobs to dequeue</param>
    /// <returns>List of dequeued job records</returns>
    public async Task<IList<JobRawModel>> DequeueSavePendingJobsAsync(string bucketId, int numberOfJobs)
    {
        if (IsAutoDequeueForSaving)
        {
            return new List<JobRawModel>();
        }

        var fullBucketAddressId = FullBucketAddressIdsUtil.GetJobSavePendingBucketAddress(bucketId);
        var messages = await savePendingRepository.DequeueMessagesAsync(fullBucketAddressId, numberOfJobs);

        var results = new List<JobRawModel>();
        foreach (var message in messages)
        {
            try
            {
                var job = InternalJobMasterSerializer.Deserialize<JobRawModel>(message.Payload);
                if (job != null)
                {
                    results.Add(job);
                }
            }
            catch (JsonException e)
            {
                logger.Critical($"Failed to deserialize job. Payload: {message.Payload}", JobMasterLogSubjectType.Bucket, bucketId, exception: e);
            }
        }

        return results;
    }

    public bool IsAutoDequeueForSaving => savePendingRepository.IsAutoDequeue;
    
    public async Task<IList<JobRawModel>> DequeueToProcessingAsync(string bucketId, int numberOfJobs, DateTime? scheduleTo)
    {
        if (IsAutoDequeueForProcessing)
        {
            return new List<JobRawModel>();
        }

        var fullBucketAddressId = FullBucketAddressIdsUtil.GetJobProcessingBucketAddress(bucketId);
        var messages = await processingRepository.DequeueMessagesAsync(fullBucketAddressId, numberOfJobs, scheduleTo);

        var results = new List<JobRawModel>();
        foreach (var message in messages)
        {
            try
            {
                var job = InternalJobMasterSerializer.Deserialize<JobRawModel>(message.Payload);
                results.Add(job);
            }
            catch (JsonException e)
            {
                logger.Critical($"Failed to deserialize job. Payload: {message.Payload}", JobMasterLogSubjectType.Bucket, bucketId, exception: e);
            }
        }

        return results;
    }

    public bool IsAutoDequeueForProcessing => processingRepository.IsAutoDequeue;
    
    public async Task<bool> HasJobsAsync(string bucketId)
    {
        var savePendingAddress = FullBucketAddressIdsUtil.GetJobSavePendingBucketAddress(bucketId);
        var processingAddress = FullBucketAddressIdsUtil.GetJobProcessingBucketAddress(bucketId);
        var recurringScheduleAddress = FullBucketAddressIdsUtil.GetRecurringScheduleSavePendingBucketAddress(bucketId);

        return await savePendingRepository.HasJobsAsync(savePendingAddress) ||
               await savePendingRepository.HasJobsAsync(recurringScheduleAddress) ||
               await processingRepository.HasJobsAsync(processingAddress);
    }

    public async Task CreateBucketAsync(string bucketId)
    {
        var savePendingAddress = FullBucketAddressIdsUtil.GetJobSavePendingBucketAddress(bucketId);
        var processingAddress = FullBucketAddressIdsUtil.GetJobProcessingBucketAddress(bucketId);
        var recurringScheduleAddress = FullBucketAddressIdsUtil.GetRecurringScheduleSavePendingBucketAddress(bucketId);

        await savePendingRepository.CreateBucketAsync(savePendingAddress);
        await savePendingRepository.CreateBucketAsync(recurringScheduleAddress);
        await processingRepository.CreateBucketAsync(processingAddress); 
    }
    
    public async Task DestroyBucketAsync(string bucketId)
    {
        var savePendingAddress = FullBucketAddressIdsUtil.GetJobSavePendingBucketAddress(bucketId);
        var processingAddress = FullBucketAddressIdsUtil.GetJobProcessingBucketAddress(bucketId);
        var recurringScheduleAddress = FullBucketAddressIdsUtil.GetRecurringScheduleSavePendingBucketAddress(bucketId);

        await savePendingRepository.DestroyBucketAsync(savePendingAddress);
        await savePendingRepository.DestroyBucketAsync(recurringScheduleAddress);
        await processingRepository.DestroyBucketAsync(processingAddress);
    }

    

    private static string GetCorrelationId(JobRawModel job)
    {
        // CorrelationId: JobId in 'N' format (32 digits, no hyphens)
        return job.Id.ToString("N");
    }

    private static string GetCorrelationId(RecurringScheduleRawModel recur)
    {
        // CorrelationId: RecurringSchedule Id in 'N' format (32 digits, no hyphens)
        return recur.Id.ToString("N");
    }

    private void EnforceDispatchSizeLimit(string payload, string correlationId)
    {
        var estimatedByteSize = JobMasterRawMessage.CalcEstimateByteSize(payload, correlationIdLength: correlationId.Length, clusterIdLength: ClusterConnConfig.ClusterId.Length);
        var maxMessageSize = masterClusterConfigurationService.Get()?.MaxMessageByteSize ??
                             new ClusterConfigurationModel(ClusterConnConfig.ClusterId).MaxMessageByteSize;

        if (estimatedByteSize > maxMessageSize)
        { 
            throw new ArgumentException($"Message size {estimatedByteSize} exceeds maximum allowed size {maxMessageSize}. Consider storing large data externally and passing a reference.");
        }
    }
}