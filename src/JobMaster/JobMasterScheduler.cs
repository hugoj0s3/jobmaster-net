using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Services;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster;

/// <summary>
/// Singleton implementation of <see cref="IJobMasterScheduler"/>.
/// Prefer injecting <see cref="IJobMasterScheduler"/> from the DI container over using <see cref="Instance"/> directly.
/// </summary>
internal class JobMasterScheduler : IJobMasterScheduler, IJobMasterSchedulerAdvanced
{
    private static IJobMasterRuntime JobMasterRuntime => JobMasterRuntimeSingleton.Instance;

    private JobMasterScheduler()
    {
    }

    /// <summary>The global singleton instance of the scheduler.</summary>
    public static IJobMasterScheduler Instance { get; } = new JobMasterScheduler();

    public IJobMasterSchedulerAdvanced Advanced => this;

    public JobScheduleReceipt OnceNow<T>(
        IWriteableMessageData? msgData = null, 
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null, 
        int? maxNumberOfRetries = null, 
        IWritableMetadata? metadata = null) where T : IJobMasterHandler
    {
       
        var job = NewJob<T>(clusterId, msgData, priority, timeout, maxNumberOfRetries, metadata, DateTime.UtcNow, workerLane);
        var jobRawModel = job.ToModel();
        EnsureCanSave(jobRawModel);
        
        var bucket = SaveJob(jobRawModel);
        return ToReceipt(jobRawModel, bucket);
    }
    
    public async Task<JobScheduleReceipt> OnceNowAsync<T>(
        IWriteableMessageData? msgData = null, 
        string? clusterId = null,
        JobMasterPriority? priority = null, 
        string? workerLane = null,
        TimeSpan? timeout = null, 
        int? maxNumberOfRetries = null, 
        IWritableMetadata? metadata = null) where T : IJobMasterHandler
    {
        var job = NewJob<T>(clusterId, msgData, priority, timeout, maxNumberOfRetries, metadata, DateTime.UtcNow, workerLane);
        var jobRawModel = job.ToModel();
        EnsureCanSave(jobRawModel);
        
        var bucket = await SaveJobAsync(jobRawModel);
        
        return ToReceipt(jobRawModel, bucket);
    }

    public JobScheduleReceipt OnceAt<T>(
        DateTime dateTime, 
        IWriteableMessageData? msgData = null, 
        string? clusterId = null,
        JobMasterPriority? priority = null, 
        string? workerLane = null,
        TimeSpan? timeout = null, 
        int? maxNumberOfRetries = null, 
        IWritableMetadata? metadata = null) where T : IJobMasterHandler
    {
        var job = NewJob<T>(clusterId, msgData, priority, timeout, maxNumberOfRetries, metadata, dateTime, workerLane);
        var jobRawModel = job.ToModel();
        EnsureCanSave(jobRawModel);
        var bucket = SaveJob(jobRawModel);
        
        return ToReceipt(jobRawModel, bucket);
    }

    public JobScheduleReceipt OnceAfter<T>(
        TimeSpan after, 
        IWriteableMessageData? msgData = null, 
        string? clusterId = null,
        JobMasterPriority? priority = null, 
        string? workerLane = null,
        TimeSpan? timeout = null, 
        int? maxNumberOfRetries = null, 
        IWritableMetadata? metadata = null) where T : IJobMasterHandler
    {
        var scheduledAt = DateTime.UtcNow.Add(after);
        var job = NewJob<T>(clusterId, msgData, priority, timeout, maxNumberOfRetries, metadata, scheduledAt, workerLane);
        var jobRawModel = job.ToModel();
        EnsureCanSave(jobRawModel);
        var bucket = SaveJob(jobRawModel);
        
        return ToReceipt(jobRawModel, bucket);
    }

    public async Task<JobScheduleReceipt> OnceAtAsync<T>(
        DateTime scheduledAt, 
        IWriteableMessageData? msgData = null, 
        string? clusterId = null,
        JobMasterPriority? priority = null, 
        string? workerLane = null,
        TimeSpan? timeout = null, 
        int? maxNumberOfRetries = null, 
        IWritableMetadata? metadata = null) where T : IJobMasterHandler
    {
        var job = NewJob<T>(clusterId, msgData, priority, timeout, maxNumberOfRetries, metadata, scheduledAt, workerLane);
        var jobRawModel = job.ToModel();
        EnsureCanSave(jobRawModel);
        
        var bucket = await SaveJobAsync(jobRawModel);
        return ToReceipt(jobRawModel, bucket);
    }

    public async Task<JobScheduleReceipt> OnceAfterAsync<T>(TimeSpan after, IWriteableMessageData? msgData = null, string? clusterId = null, JobMasterPriority? priority = null, string? workerLane = null, TimeSpan? timeout = null, int? maxNumberOfRetries = null, IWritableMetadata? metadata = null) where T : IJobMasterHandler
    {
        var scheduledAt = DateTime.UtcNow.Add(after);
        var job = NewJob<T>(clusterId, msgData, priority, timeout, maxNumberOfRetries, metadata, scheduledAt, workerLane);
        var jobRawModel = job.ToModel();
        EnsureCanSave(jobRawModel);
        
        var bucket = await SaveJobAsync(jobRawModel);
        return ToReceipt(jobRawModel, bucket);
    }

    public RecurringScheduleReceipt Recurring<T>(IRecurrenceCompiledExpr expression, IWriteableMessageData? data = null, string? clusterId = null, JobMasterPriority? priority = null, string? workerLane = null, TimeSpan? timeout = null, int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null, DateTime? startAfter = null, DateTime? endBefore = null) where T : IJobMasterHandler
    {
        var recurring = NewRecurSchedule<T>(clusterId, data, expression, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore, workerLane);
        var raw = recurring.ToModel();
        EnsureCanSave(raw);
        var bucket = SaveRecurringSchedule(raw);
        return ToReceipt(raw, bucket);
    }

    public async Task<RecurringScheduleReceipt> RecurringAsync<T>(IRecurrenceCompiledExpr expression, IWriteableMessageData? data = null, string? clusterId = null, JobMasterPriority? priority = null,  string? workerLane = null, TimeSpan? timeout = null, int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null, DateTime? startAfter = null, DateTime? endBefore = null) where T : IJobMasterHandler
    {
        var recurring = NewRecurSchedule<T>(clusterId, data, expression, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore, workerLane);
        var raw = recurring.ToModel();
        EnsureCanSave(raw);
        var bucket = await SaveRecurringScheduleAsync(raw);
        
        return ToReceipt(raw, bucket);
    }

    public RecurringScheduleReceipt Recurring<T>(string expressionTypeId, string expression, IWriteableMessageData? data = null, string? clusterId = null, JobMasterPriority? priority = null, string? workerLane = null, TimeSpan? timeout = null, int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null, DateTime? startAfter = null, DateTime? endBefore = null) where T : IJobMasterHandler
    {
        var compiled = RecurrenceExprCompiler.Compile(expressionTypeId, expression);
        var recurring = NewRecurSchedule<T>(clusterId, data, compiled, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore, workerLane);
        var raw = recurring.ToModel();
        EnsureCanSave(raw);
        var bucket = SaveRecurringSchedule(raw);
        return ToReceipt(raw, bucket);
    }

    public async Task<RecurringScheduleReceipt> RecurringAsync<T>(string expressionTypeId, string expression, IWriteableMessageData? data = null, string? clusterId = null, JobMasterPriority? priority = null, string? workerLane = null,  TimeSpan? timeout = null, int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null, DateTime? startAfter = null, DateTime? endBefore = null) where T : IJobMasterHandler
    {
        var compiled = RecurrenceExprCompiler.Compile(expressionTypeId, expression);
        var recurring = NewRecurSchedule<T>(clusterId, data, compiled, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore, workerLane);
        var raw = recurring.ToModel();
        EnsureCanSave(raw);
        var bucket = await SaveRecurringScheduleAsync(raw);
        return ToReceipt(raw, bucket);
    }

    public Task<bool> CancelJobAsync(Guid jobId, string? clusterId = null)
    {
        var schedulerClusterAware = EnsureGetSchedulerClusterAware(clusterId);
        return schedulerClusterAware.CancelJobAsync(jobId);
    }

    public bool CancelJob(Guid jobId, string? clusterId = null)
    {
        var schedulerClusterAware = EnsureGetSchedulerClusterAware(clusterId);
        return schedulerClusterAware.CancelJob(jobId);
    }

    public Task<bool> CancelRecurringAsync(Guid recurringScheduleId, string? clusterId = null)
    {
        var schedulerClusterAware = EnsureGetSchedulerClusterAware(clusterId);
        return schedulerClusterAware.CancelRecurringAsync(recurringScheduleId);
    }

    public bool CancelRecurring(Guid recurringScheduleId, string? clusterId = null)
    {
        var schedulerClusterAware = EnsureGetSchedulerClusterAware(clusterId);
        return schedulerClusterAware.CancelRecurring(recurringScheduleId);
    }

    public Task<bool> RescheduleAsync(Guid jobId, DateTime scheduledAt, string? clusterId = null)
    {
        var schedulerClusterAware = EnsureGetSchedulerClusterAware(clusterId);
        return schedulerClusterAware.RescheduleAsync(jobId, scheduledAt);
    }

    public bool Reschedule(Guid jobId, DateTime scheduledAt, string? clusterId = null)
    {
        var schedulerClusterAware = EnsureGetSchedulerClusterAware(clusterId);
        return schedulerClusterAware.Reschedule(jobId, scheduledAt);
    }
    
    // The generic Advanced.OnceNow<TDefinition>/OnceAt<TDefinition>/OnceAfter<TDefinition> (+ Async) methods
    // are implemented explicitly below: a single class cannot implicitly implement both this interface's
    // OnceNow<T>() (constrained to IJobMasterHandler) and IJobMasterSchedulerAdvanced's OnceNow<TDefinition>()
    // (constrained to JobDefinitionConfigAttribute) under the same public member name — C# requires matching
    // constraints for implicit implementations (CS0425/CS0111). Explicit implementation sidesteps this since
    // they're only reachable through IJobMasterSchedulerAdvanced (i.e. via the Advanced property), which is
    // the only intended access path anyway.

    JobScheduleReceipt IJobMasterSchedulerAdvanced.OnceNow<TDefinition>(
        IWriteableMessageData? msgData,
        string? clusterId,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata)
        => OnceNow(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), msgData, clusterId);

    public JobScheduleReceipt OnceNow(
        JobDefinitionConfig config,
        IWriteableMessageData? msgData = null,
        string? clusterId = null)
    {
        var job = NewJob(clusterId, config, msgData, DateTime.UtcNow);
        var jobRawModel = job.ToModel();
        EnsureCanSave(jobRawModel);

        var bucket = SaveJob(jobRawModel);

        return ToReceipt(jobRawModel, bucket);
    }

    async Task<JobScheduleReceipt> IJobMasterSchedulerAdvanced.OnceNowAsync<TDefinition>(
        IWriteableMessageData? msgData,
        string? clusterId,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata)
        => await OnceNowAsync(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), msgData, clusterId);

    public async Task<JobScheduleReceipt> OnceNowAsync(
        JobDefinitionConfig config,
        IWriteableMessageData? msgData = null,
        string? clusterId = null)
    {
        var job = NewJob(clusterId, config, msgData, DateTime.UtcNow);
        var jobRawModel = job.ToModel();
        EnsureCanSave(jobRawModel);

        var bucket = await SaveJobAsync(jobRawModel);

        return ToReceipt(jobRawModel, bucket);
    }

    JobScheduleReceipt IJobMasterSchedulerAdvanced.OnceAt<TDefinition>(
        DateTime dateTime,
        IWriteableMessageData? msgData,
        string? clusterId,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata)
        => OnceAt(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), dateTime, msgData, clusterId);

    public JobScheduleReceipt OnceAt(
        JobDefinitionConfig config,
        DateTime dateTime,
        IWriteableMessageData? msgData = null,
        string? clusterId = null)
    {
        var job = NewJob(clusterId, config, msgData, dateTime);
        var jobRawModel = job.ToModel();
        EnsureCanSave(jobRawModel);
        var bucket = SaveJob(jobRawModel);

        return ToReceipt(jobRawModel, bucket);
    }

    async Task<JobScheduleReceipt> IJobMasterSchedulerAdvanced.OnceAtAsync<TDefinition>(
        DateTime dateTime,
        IWriteableMessageData? msgData,
        string? clusterId,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata)
        => await OnceAtAsync(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), dateTime, msgData, clusterId);

    public async Task<JobScheduleReceipt> OnceAtAsync(
        JobDefinitionConfig config,
        DateTime dateTime,
        IWriteableMessageData? msgData = null,
        string? clusterId = null)
    {
        var job = NewJob(clusterId, config, msgData, dateTime);
        var jobRawModel = job.ToModel();
        EnsureCanSave(jobRawModel);

        var bucket = await SaveJobAsync(jobRawModel);
        return ToReceipt(jobRawModel, bucket);
    }

    JobScheduleReceipt IJobMasterSchedulerAdvanced.OnceAfter<TDefinition>(
        TimeSpan after,
        IWriteableMessageData? msgData,
        string? clusterId,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata)
        => OnceAfter(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), after, msgData, clusterId);

    public JobScheduleReceipt OnceAfter(
        JobDefinitionConfig config,
        TimeSpan after,
        IWriteableMessageData? msgData = null,
        string? clusterId = null)
    {
        var scheduledAt = DateTime.UtcNow.Add(after);
        var job = NewJob(clusterId, config, msgData, scheduledAt);
        var jobRawModel = job.ToModel();
        EnsureCanSave(jobRawModel);
        var bucket = SaveJob(jobRawModel);

        return ToReceipt(jobRawModel, bucket);
    }

    async Task<JobScheduleReceipt> IJobMasterSchedulerAdvanced.OnceAfterAsync<TDefinition>(
        TimeSpan after,
        IWriteableMessageData? msgData,
        string? clusterId,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata)
        => await OnceAfterAsync(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), after, msgData, clusterId);

    public async Task<JobScheduleReceipt> OnceAfterAsync(
        JobDefinitionConfig config,
        TimeSpan after,
        IWriteableMessageData? msgData = null,
        string? clusterId = null)
    {
        var scheduledAt = DateTime.UtcNow.Add(after);
        var job = NewJob(clusterId, config, msgData, scheduledAt);
        var jobRawModel = job.ToModel();
        EnsureCanSave(jobRawModel);

        var bucket = await SaveJobAsync(jobRawModel);
        return ToReceipt(jobRawModel, bucket);
    }

    RecurringScheduleReceipt IJobMasterSchedulerAdvanced.Recurring<TDefinition>(
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data,
        string? clusterId,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata,
        DateTime? startAfter,
        DateTime? endBefore)
        => Recurring(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), expression, data, clusterId, startAfter, endBefore);

    public RecurringScheduleReceipt Recurring(
        JobDefinitionConfig config,
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null)
    {
        var recurring = NewRecurSchedule(clusterId, config, data, expression, startAfter, endBefore);
        var raw = recurring.ToModel();
        EnsureCanSave(raw);
        var bucket = SaveRecurringSchedule(raw);
        return ToReceipt(raw, bucket);
    }

    async Task<RecurringScheduleReceipt> IJobMasterSchedulerAdvanced.RecurringAsync<TDefinition>(
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data,
        string? clusterId,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata,
        DateTime? startAfter,
        DateTime? endBefore)
        => await RecurringAsync(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), expression, data, clusterId, startAfter, endBefore);

    public async Task<RecurringScheduleReceipt> RecurringAsync(
        JobDefinitionConfig config,
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null)
    {
        var recurring = NewRecurSchedule(clusterId, config, data, expression, startAfter, endBefore);
        var raw = recurring.ToModel();
        EnsureCanSave(raw);
        var bucket = await SaveRecurringScheduleAsync(raw);
        return ToReceipt(raw, bucket);
    }

    RecurringScheduleReceipt IJobMasterSchedulerAdvanced.Recurring<TDefinition>(
        string expressionTypeId,
        string expression,
        IWriteableMessageData? data,
        string? clusterId,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata,
        DateTime? startAfter,
        DateTime? endBefore)
        => Recurring(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), expressionTypeId, expression, data, clusterId, startAfter, endBefore);

    public RecurringScheduleReceipt Recurring(
        JobDefinitionConfig config,
        string expressionTypeId,
        string expression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null)
    {
        var compiled = RecurrenceExprCompiler.Compile(expressionTypeId, expression);
        var recurring = NewRecurSchedule(clusterId, config, data, compiled, startAfter, endBefore);
        var raw = recurring.ToModel();
        EnsureCanSave(raw);
        var bucket = SaveRecurringSchedule(raw);
        return ToReceipt(raw, bucket);
    }

    async Task<RecurringScheduleReceipt> IJobMasterSchedulerAdvanced.RecurringAsync<TDefinition>(
        string expressionTypeId,
        string expression,
        IWriteableMessageData? data,
        string? clusterId,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata,
        DateTime? startAfter,
        DateTime? endBefore)
        => await RecurringAsync(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), expressionTypeId, expression, data, clusterId, startAfter, endBefore);

    public async Task<RecurringScheduleReceipt> RecurringAsync(
        JobDefinitionConfig config,
        string expressionTypeId,
        string expression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null)
    {
        var compiled = RecurrenceExprCompiler.Compile(expressionTypeId, expression);
        var recurring = NewRecurSchedule(clusterId, config, data, compiled, startAfter, endBefore);
        var raw = recurring.ToModel();
        EnsureCanSave(raw);
        var bucket = await SaveRecurringScheduleAsync(raw);
        return ToReceipt(raw, bucket);
    }

    /// <summary>
    /// Returns <paramref name="config"/> unchanged when no override is set, otherwise a new
    /// <see cref="JobDefinitionConfig"/> with the same <see cref="JobDefinitionConfig.JobDefinitionId"/>
    /// and each override applied on top of <paramref name="config"/>'s own values. Used only by the
    /// <typeparamref name="TDefinition"/>-generic Advanced overloads, since <paramref name="config"/> there
    /// comes from <typeparamref name="TDefinition"/>'s fixed, static <see cref="JobDefinitionConfig"/>
    /// rather than being built fresh per call.
    /// </summary>
    private static JobDefinitionConfig ApplyOverrides(
        JobDefinitionConfig config,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata)
    {
        if (priority is null && workerLane is null && timeout is null && maxNumberOfRetries is null && metadata is null)
        {
            return config;
        }

        return new JobDefinitionConfig(
            config.JobDefinitionId,
            priority: priority ?? config.Priority,
            timeout: timeout ?? config.Timeout,
            maxNumberOfRetries: maxNumberOfRetries ?? config.MaxNumberOfRetries,
            workerLane: workerLane ?? config.WorkerLane,
            metadata: metadata ?? config.Metadata,
            clusterId: config.ClusterId);
    }

    private static string ResolveClusterId(string? clusterId)
    {
        if (clusterId != null)
        {
            // Matched ignoring case, but the job/schedule must carry the configured casing.
            return JobMasterClusterConnectionConfig.ToConfiguredClusterId(clusterId);
        }

        if (JobMasterClusterConnectionConfig.Default == null)
        {
            throw new KeyNotFoundException("Default cluster config not found");
        }

        return JobMasterClusterConnectionConfig.Default.ClusterId;
    }

    private Job NewJob(
        string? clusterId,
        JobDefinitionConfig config,
        IWriteableMessageData? data,
        DateTime? scheduledAt)
    {
        clusterId = ResolveClusterId(clusterId ?? config.ClusterId);
        var clusterConfiguration = EnsureGetMasterClusterConfigurationService(clusterId).Get();
        return Job.New(
            clusterId,
            config,
            data,
            scheduledAt,
            triggerSourceType: JobMasterTriggerSourceType.Once,
            masterConfig: clusterConfiguration);
    }

    private RecurringSchedule NewRecurSchedule(
        string? clusterId,
        JobDefinitionConfig config,
        IWriteableMessageData? values,
        IRecurrenceCompiledExpr expression,
        DateTime? startAfter,
        DateTime? endBefore)
    {
        clusterId = ResolveClusterId(clusterId ?? config.ClusterId);
        return RecurringSchedule.New(
            clusterId,
            config,
            values,
            expression,
            RecurringScheduleType.Dynamic,
            staticDefinitionId: null,
            startAfter,
            endBefore);
    }

    private RecurringSchedule NewRecurSchedule<T>(
        string? clusterId,
        IWriteableMessageData? values, 
        IRecurrenceCompiledExpr expression, 
        JobMasterPriority? priority,
        TimeSpan? timeout, 
        int? maxNumberOfRetries, 
        IWritableMetadata? metadata,
        DateTime? startAfter,
        DateTime? endBefore,
        string? workerLane) where T : IJobMasterHandler
    {
        clusterId = ResolveClusterId(JobUtil.GetClusterId(typeof(T), clusterId));
        var rec = RecurringSchedule.New<T>(
            clusterId,
            values,
            expression,
            priority,
            timeout,
            maxNumberOfRetries,
            metadata,
            RecurringScheduleType.Dynamic,
            staticDefinitionId: null,
            startAfter,
            endBefore,
            workerLane);
        return rec;
    }
    
    private Job NewJob<T>(
        string? clusterId,
        IWriteableMessageData? data,
        JobMasterPriority? priority,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? writableMetadata,
        DateTime? scheduledAt,
        string? workerLane) where T : IJobMasterHandler
    {
        clusterId = ResolveClusterId(JobUtil.GetClusterId(typeof(T), clusterId));
        var clusterConfiguration = EnsureGetMasterClusterConfigurationService(clusterId).Get();
        return Job.New<T>(
            clusterId,
            data,
            scheduledAt,
            priority: priority,
            timeout: timeout,
            maxNumberOfRetries: maxNumberOfRetries,
            writableMetadata: writableMetadata,
            triggerSourceType: JobMasterTriggerSourceType.Once,
            masterConfig: clusterConfiguration,
            workerLane: workerLane);
    }
    
    

    // Validates against the model's already-resolved ClusterId (explicit → definition/attribute → default),
    // never the raw clusterId argument, which would silently fall back to the default cluster.
    private void EnsureCanSave(RecurringScheduleRawModel recurringSchMd)
    {
        if (recurringSchMd.MaxNumberOfRetries > JobMasterConstants.MaxAllowedRetries)
        {
            throw new ArgumentException($"MaxNumberOfRetries must be less than or equal to {JobMasterConstants.MaxAllowedRetries}.");
        }

        EnsureCanSave(recurringSchMd.ClusterId);
        var config = EnsureGetMasterClusterConfigurationService(recurringSchMd.ClusterId).Get();
        if (config == null)
            throw new KeyNotFoundException("Cluster config not found");

        if (recurringSchMd.Priority.HasValue &&
            JobMasterClusterConnectionConfig.TryGet(recurringSchMd.ClusterId, includeNotReady: true)
                ?.IsPriorityDisabled(recurringSchMd.Priority.Value) == true)
        {
            throw new InvalidOperationException(
                $"Priority {recurringSchMd.Priority.Value} is disabled on cluster '{recurringSchMd.ClusterId}'. " +
                $"Cannot schedule recurring schedule '{recurringSchMd.JobDefinitionId}'.");
        }
    }

    private void EnsureCanSave(JobRawModel job)
    {
        if (job.MaxNumberOfRetries > JobMasterConstants.MaxAllowedRetries)
        {
            throw new ArgumentException($"MaxNumberOfRetries must be less than or equal to {JobMasterConstants.MaxAllowedRetries}.");
        }

        EnsureCanSave(job.ClusterId);
        var config = EnsureGetMasterClusterConfigurationService(job.ClusterId).Get();
        if (config == null)
            throw new KeyNotFoundException("Cluster config not found");

        if (JobMasterClusterConnectionConfig.TryGet(job.ClusterId, includeNotReady: true)?.IsPriorityDisabled(job.Priority) == true)
            throw new InvalidOperationException(
                $"Priority {job.Priority} is disabled on cluster '{job.ClusterId}'. " +
                $"Cannot schedule job '{job.JobDefinitionId}'.");
    }

    private void EnsureCanSave(string? cluserId)
    {
        if (JobMasterRuntime == null || !JobMasterRuntime.Started)
            throw new InvalidOperationException("JobMasterRuntime is not initialized");

        cluserId = ResolveClusterId(cluserId);
        var config = EnsureGetMasterClusterConfigurationService(cluserId).Get();
        if (config == null)
            throw new KeyNotFoundException("Cluster config not found");
        
        EnsureGetSchedulerClusterAware(cluserId);
    }
    
    private IJobMasterSchedulerClusterAware EnsureGetSchedulerClusterAware(string? clusterId)
    {
        if (JobMasterRuntime == null || !JobMasterRuntime.Started)
            throw new InvalidOperationException("JobMasterRuntime is not initialized");

        clusterId = ResolveClusterId(clusterId);
        var factory = JobMasterClusterAwareComponentFactories.GetFactory(clusterId);

        return factory.GetComponent<IJobMasterSchedulerClusterAware>();
    }

    private IMasterClusterConfigurationService EnsureGetMasterClusterConfigurationService(string? clusterId)
    {
        clusterId = ResolveClusterId(clusterId);
        var factory = JobMasterClusterAwareComponentFactories.GetFactory(clusterId);
        return factory.GetComponent<IMasterClusterConfigurationService>();
    }
    
    
    private BucketModel? SaveJob(JobRawModel jobRawModel)
    {
        var service = EnsureGetSchedulerClusterAware(jobRawModel.ClusterId);
        return service.Schedule(jobRawModel);
    }

    private BucketModel? SaveRecurringSchedule(RecurringScheduleRawModel recurringRawModel)
    {
        var service = EnsureGetSchedulerClusterAware(recurringRawModel.ClusterId);
        return service.Schedule(recurringRawModel);
    }

    private Task<BucketModel?> SaveJobAsync(JobRawModel jobRawModel)
    {
        var service = EnsureGetSchedulerClusterAware(jobRawModel.ClusterId);
        return service.ScheduleAsync(jobRawModel);
    }

    private Task<BucketModel?> SaveRecurringScheduleAsync(RecurringScheduleRawModel recurringRawModel)
    {
        var service = EnsureGetSchedulerClusterAware(recurringRawModel.ClusterId);
        return service.ScheduleAsync(recurringRawModel);
    }

    public async Task ConfirmOnMasterAsync(IEnumerable<JobScheduleReceipt> receipts)
    {
        var accepted = receipts.Where(r => r.Status == ScheduleReceiptStatus.AcceptedOnTransport);
        foreach (var cluster in accepted.GroupBy(r => r.ClusterId, StringComparer.OrdinalIgnoreCase))
        {
            var service = EnsureGetSchedulerClusterAware(cluster.Key);
            await service.ConfirmJobsOnMasterAsync(cluster.Select(r => r.model).ToList());
        }
    }

    public async Task ConfirmOnMasterAsync(IEnumerable<RecurringScheduleReceipt> receipts)
    {
        var accepted = receipts.Where(r => r.Status == ScheduleReceiptStatus.AcceptedOnTransport);
        foreach (var cluster in accepted.GroupBy(r => r.ClusterId, StringComparer.OrdinalIgnoreCase))
        {
            var service = EnsureGetSchedulerClusterAware(cluster.Key);
            await service.ConfirmRecurringSchedulesOnMasterAsync(cluster.Select(r => r.model).ToList());
        }
    }

    // Only accepted on the transport: try to save it on master already cancelled first; if it's already there
    // (e.g. the agent saved it), cancel it by id like CancelJobAsync.
    public async Task<bool> CancelJobAsync(JobScheduleReceipt receipt)
    {
        var service = EnsureGetSchedulerClusterAware(receipt.ClusterId);
        if (receipt.Status == ScheduleReceiptStatus.AcceptedOnTransport &&
            await service.InsertCancelledJobAsync(receipt.model))
        {
            return true;
        }

        return await service.CancelJobAsync(receipt.JobId);
    }

    public async Task<bool> CancelRecurringAsync(RecurringScheduleReceipt receipt)
    {
        var service = EnsureGetSchedulerClusterAware(receipt.ClusterId);
        if (receipt.Status == ScheduleReceiptStatus.AcceptedOnTransport &&
            await service.InsertCancelledRecurringScheduleAsync(receipt.model))
        {
            return true;
        }

        return await service.CancelRecurringAsync(receipt.RecurringScheduleId);
    }

    // bucket: where the cluster-aware scheduler handed it on the agent transport; null = stored on master.
    private static JobScheduleReceipt ToReceipt(JobRawModel model, BucketModel? bucket) =>
        new(StatusOf(bucket), model);

    private static RecurringScheduleReceipt ToReceipt(RecurringScheduleRawModel model, BucketModel? bucket) =>
        new(StatusOf(bucket), model);

    private static ScheduleReceiptStatus StatusOf(BucketModel? bucket) =>
        bucket is null ? ScheduleReceiptStatus.StoredOnMaster : ScheduleReceiptStatus.AcceptedOnTransport;
}
