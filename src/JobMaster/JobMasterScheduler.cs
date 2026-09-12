using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Services;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster;

/// <summary>
/// Singleton implementation of <see cref="IJobMasterScheduler"/>.
/// Prefer injecting <see cref="IJobMasterScheduler"/> from the DI container over using <see cref="Instance"/> directly.
/// </summary>
public class JobMasterScheduler : IJobMasterScheduler, IJobMasterSchedulerAdvanced
{
    private static IJobMasterRuntime JobMasterRuntime => JobMasterRuntimeSingleton.Instance;

    private JobMasterScheduler()
    {
    }

    /// <summary>The global singleton instance of the scheduler.</summary>
    public static IJobMasterScheduler Instance { get; } = new JobMasterScheduler();

    public IJobMasterSchedulerAdvanced Advanced => this;

    public JobContext OnceNow<T>(
        IWriteableMessageData? msgData = null, 
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null, 
        int? maxNumberOfRetries = null, 
        IWritableMetadata? metadata = null, 
        string? clusterId = null) where T : IJobMasterHandler
    {
       
        var job = NewJob<T>(clusterId, msgData, priority, timeout, maxNumberOfRetries, metadata, DateTime.UtcNow, workerLane);
        var jobRawModel = job.ToModel();
        EnsureCanSave(clusterId, jobRawModel);
        
        SaveJob(jobRawModel);
        
        return JobConvertUtil.ToJobContext(job);
    }
    
    public async Task<JobContext> OnceNowAsync<T>(
        IWriteableMessageData? msgData = null, 
        JobMasterPriority? priority = null, 
        string? workerLane = null,
        TimeSpan? timeout = null, 
        int? maxNumberOfRetries = null, 
        IWritableMetadata? metadata = null, 
        string? clusterId = null) where T : IJobMasterHandler
    {
        var job = NewJob<T>(clusterId, msgData, priority, timeout, maxNumberOfRetries, metadata, DateTime.UtcNow, workerLane);
        var jobRawModel = job.ToModel();
        EnsureCanSave(clusterId, jobRawModel);
        
        await SaveJobAsync(jobRawModel);
        
        return JobConvertUtil.ToJobContext(job);
    }

    public JobContext OnceAt<T>(
        DateTime dateTime, 
        IWriteableMessageData? msgData = null, 
        JobMasterPriority? priority = null, 
        string? workerLane = null,
        TimeSpan? timeout = null, 
        int? maxNumberOfRetries = null, 
        IWritableMetadata? metadata = null,
        string? clusterId = null) where T : IJobMasterHandler
    {
        var job = NewJob<T>(clusterId, msgData, priority, timeout, maxNumberOfRetries, metadata, dateTime, workerLane);
        var jobRawModel = job.ToModel();
        EnsureCanSave(clusterId, jobRawModel);
        SaveJob(jobRawModel);
        
        return JobConvertUtil.ToJobContext(job);
    }

    public JobContext OnceAfter<T>(
        TimeSpan after, 
        IWriteableMessageData? msgData = null, 
        JobMasterPriority? priority = null, 
        string? workerLane = null,
        TimeSpan? timeout = null, 
        int? maxNumberOfRetries = null, 
        IWritableMetadata? metadata = null,
        string? clusterId = null) where T : IJobMasterHandler
    {
        var scheduledAt = DateTime.UtcNow.Add(after);
        var job = NewJob<T>(clusterId, msgData, priority, timeout, maxNumberOfRetries, metadata, scheduledAt, workerLane);
        var jobRawModel = job.ToModel();
        EnsureCanSave(clusterId, jobRawModel);
        SaveJob(jobRawModel);
        
        return JobConvertUtil.ToJobContext(job);
    }

    public async Task<JobContext> OnceAtAsync<T>(
        DateTime scheduledAt, 
        IWriteableMessageData? msgData = null, 
        JobMasterPriority? priority = null, 
        string? workerLane = null,
        TimeSpan? timeout = null, 
        int? maxNumberOfRetries = null, 
        IWritableMetadata? metadata = null,
        string? clusterId = null) where T : IJobMasterHandler
    {
        var job = NewJob<T>(clusterId, msgData, priority, timeout, maxNumberOfRetries, metadata, scheduledAt, workerLane);
        var jobRawModel = job.ToModel();
        EnsureCanSave(clusterId, jobRawModel);
        
        await SaveJobAsync(jobRawModel);
        return JobConvertUtil.ToJobContext(job);
    }

    public async Task<JobContext> OnceAfterAsync<T>(TimeSpan after, IWriteableMessageData? msgData = null, JobMasterPriority? priority = null, string? workerLane = null, TimeSpan? timeout = null, int? maxNumberOfRetries = null, IWritableMetadata? metadata = null,
        string? clusterId = null) where T : IJobMasterHandler
    {
        var scheduledAt = DateTime.UtcNow.Add(after);
        var job = NewJob<T>(clusterId, msgData, priority, timeout, maxNumberOfRetries, metadata, scheduledAt, workerLane);
        var jobRawModel = job.ToModel();
        EnsureCanSave(clusterId, jobRawModel);
        
        await SaveJobAsync(jobRawModel);
        return JobConvertUtil.ToJobContext(job);
    }

    public RecurringScheduleContext Recurring<T>(IRecurrenceCompiledExpr expression, IWriteableMessageData? data = null, JobMasterPriority? priority = null, string? workerLane = null, TimeSpan? timeout = null, int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null, DateTime? startAfter = null, DateTime? endBefore = null, string? clusterId = null) where T : IJobMasterHandler
    {
        var recurring = NewRecurSchedule<T>(clusterId, data, expression, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore, workerLane);
        var raw = recurring.ToModel();
        EnsureCanSave(clusterId, raw);
        SaveRecurringSchedule(raw);
        return RecurringScheduleConvertUtil.ToContext(recurring);
    }

    public async Task<RecurringScheduleContext> RecurringAsync<T>(IRecurrenceCompiledExpr expression, IWriteableMessageData? data = null, JobMasterPriority? priority = null,  string? workerLane = null, TimeSpan? timeout = null, int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null, DateTime? startAfter = null, DateTime? endBefore = null, string? clusterId = null) where T : IJobMasterHandler
    {
        var recurring = NewRecurSchedule<T>(clusterId, data, expression, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore, workerLane);
        var raw = recurring.ToModel();
        EnsureCanSave(clusterId, raw);
        await SaveRecurringScheduleAsync(raw);
        
        return RecurringScheduleConvertUtil.ToContext(recurring);
    }

    public RecurringScheduleContext Recurring<T>(string expressionTypeId, string expression, IWriteableMessageData? data = null, JobMasterPriority? priority = null, string? workerLane = null, TimeSpan? timeout = null, int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null, DateTime? startAfter = null, DateTime? endBefore = null, string? clusterId = null) where T : IJobMasterHandler
    {
        var compiled = RecurrenceExprCompiler.Compile(expressionTypeId, expression);
        var recurring = NewRecurSchedule<T>(clusterId, data, compiled, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore, workerLane);
        var raw = recurring.ToModel();
        EnsureCanSave(clusterId, raw);
        SaveRecurringSchedule(raw);
        return RecurringScheduleConvertUtil.ToContext(recurring);
    }

    public async Task<RecurringScheduleContext> RecurringAsync<T>(string expressionTypeId, string expression, IWriteableMessageData? data = null, JobMasterPriority? priority = null, string? workerLane = null,  TimeSpan? timeout = null, int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null, DateTime? startAfter = null, DateTime? endBefore = null, string? clusterId = null) where T : IJobMasterHandler
    {
        var compiled = RecurrenceExprCompiler.Compile(expressionTypeId, expression);
        var recurring = NewRecurSchedule<T>(clusterId, data, compiled, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore, workerLane);
        var raw = recurring.ToModel();
        EnsureCanSave(clusterId, raw);
        await SaveRecurringScheduleAsync(raw);
        return RecurringScheduleConvertUtil.ToContext(recurring);
    }

    public Task<bool> CancelJobAsync(Guid jobId, string? clusterId = null)
    {
        var schedulerClusterAware = EnsureGetSchedulerClusterAware(clusterId);
        return schedulerClusterAware.CancelJobAsync(jobId);
    }

    public bool TryCancelJob(Guid id, string? clusterId = null)
    {
        var schedulerClusterAware = EnsureGetSchedulerClusterAware(clusterId);
        return schedulerClusterAware.CancelJob(id);
    }

    public Task<bool> TryCancelRecurringAsync(Guid id, string? clusterId = null)
    {
        var schedulerClusterAware = EnsureGetSchedulerClusterAware(clusterId);
        return schedulerClusterAware.CancelRecurringAsync(id);
    }

    public bool CancelRecurring(Guid id, string? clusterId = null)
    {
        var schedulerClusterAware = EnsureGetSchedulerClusterAware(clusterId);
        return schedulerClusterAware.CancelRecurring(id);
    }

    public Task<bool> ReScheduleAsync(Guid jobId, DateTime scheduledAt, string? clusterId = null)
    {
        var schedulerClusterAware = EnsureGetSchedulerClusterAware(clusterId);
        return schedulerClusterAware.ReScheduleAsync(jobId, scheduledAt);
    }

    public bool ReSchedule(Guid jobId, DateTime scheduledAt, string? clusterId = null)
    {
        var schedulerClusterAware = EnsureGetSchedulerClusterAware(clusterId);
        return schedulerClusterAware.ReSchedule(jobId, scheduledAt);
    }
    
    // The generic Advanced.OnceNow<TDefinition>/OnceAt<TDefinition>/OnceAfter<TDefinition> (+ Async) methods
    // are implemented explicitly below: a single class cannot implicitly implement both this interface's
    // OnceNow<T>() (constrained to IJobMasterHandler) and IJobMasterSchedulerAdvanced's OnceNow<TDefinition>()
    // (constrained to JobDefinitionConfigAttribute) under the same public member name — C# requires matching
    // constraints for implicit implementations (CS0425/CS0111). Explicit implementation sidesteps this since
    // they're only reachable through IJobMasterSchedulerAdvanced (i.e. via the Advanced property), which is
    // the only intended access path anyway.

    JobContext IJobMasterSchedulerAdvanced.OnceNow<TDefinition>(
        IWriteableMessageData? msgData,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata,
        string? clusterId)
        => OnceNow(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), msgData, clusterId);

    public JobContext OnceNow(
        JobDefinitionConfig config,
        IWriteableMessageData? msgData = null,
        string? clusterId = null)
    {
        var job = NewJob(clusterId, config, msgData, DateTime.UtcNow);
        var jobRawModel = job.ToModel();
        EnsureCanSave(clusterId, jobRawModel);

        SaveJob(jobRawModel);

        return JobConvertUtil.ToJobContext(job);
    }

    async Task<JobContext> IJobMasterSchedulerAdvanced.OnceNowAsync<TDefinition>(
        IWriteableMessageData? msgData,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata,
        string? clusterId)
        => await OnceNowAsync(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), msgData, clusterId);

    public async Task<JobContext> OnceNowAsync(
        JobDefinitionConfig config,
        IWriteableMessageData? msgData = null,
        string? clusterId = null)
    {
        var job = NewJob(clusterId, config, msgData, DateTime.UtcNow);
        var jobRawModel = job.ToModel();
        EnsureCanSave(clusterId, jobRawModel);

        await SaveJobAsync(jobRawModel);

        return JobConvertUtil.ToJobContext(job);
    }

    JobContext IJobMasterSchedulerAdvanced.OnceAt<TDefinition>(
        DateTime dateTime,
        IWriteableMessageData? msgData,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata,
        string? clusterId)
        => OnceAt(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), dateTime, msgData, clusterId);

    public JobContext OnceAt(
        JobDefinitionConfig config,
        DateTime dateTime,
        IWriteableMessageData? msgData = null,
        string? clusterId = null)
    {
        var job = NewJob(clusterId, config, msgData, dateTime);
        var jobRawModel = job.ToModel();
        EnsureCanSave(clusterId, jobRawModel);
        SaveJob(jobRawModel);

        return JobConvertUtil.ToJobContext(job);
    }

    async Task<JobContext> IJobMasterSchedulerAdvanced.OnceAtAsync<TDefinition>(
        DateTime dateTime,
        IWriteableMessageData? msgData,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata,
        string? clusterId)
        => await OnceAtAsync(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), dateTime, msgData, clusterId);

    public async Task<JobContext> OnceAtAsync(
        JobDefinitionConfig config,
        DateTime dateTime,
        IWriteableMessageData? msgData = null,
        string? clusterId = null)
    {
        var job = NewJob(clusterId, config, msgData, dateTime);
        var jobRawModel = job.ToModel();
        EnsureCanSave(clusterId, jobRawModel);

        await SaveJobAsync(jobRawModel);
        return JobConvertUtil.ToJobContext(job);
    }

    JobContext IJobMasterSchedulerAdvanced.OnceAfter<TDefinition>(
        TimeSpan after,
        IWriteableMessageData? msgData,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata,
        string? clusterId)
        => OnceAfter(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), after, msgData, clusterId);

    public JobContext OnceAfter(
        JobDefinitionConfig config,
        TimeSpan after,
        IWriteableMessageData? msgData = null,
        string? clusterId = null)
    {
        var scheduledAt = DateTime.UtcNow.Add(after);
        var job = NewJob(clusterId, config, msgData, scheduledAt);
        var jobRawModel = job.ToModel();
        EnsureCanSave(clusterId, jobRawModel);
        SaveJob(jobRawModel);

        return JobConvertUtil.ToJobContext(job);
    }

    async Task<JobContext> IJobMasterSchedulerAdvanced.OnceAfterAsync<TDefinition>(
        TimeSpan after,
        IWriteableMessageData? msgData,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata,
        string? clusterId)
        => await OnceAfterAsync(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), after, msgData, clusterId);

    public async Task<JobContext> OnceAfterAsync(
        JobDefinitionConfig config,
        TimeSpan after,
        IWriteableMessageData? msgData = null,
        string? clusterId = null)
    {
        var scheduledAt = DateTime.UtcNow.Add(after);
        var job = NewJob(clusterId, config, msgData, scheduledAt);
        var jobRawModel = job.ToModel();
        EnsureCanSave(clusterId, jobRawModel);

        await SaveJobAsync(jobRawModel);
        return JobConvertUtil.ToJobContext(job);
    }

    RecurringScheduleContext IJobMasterSchedulerAdvanced.Recurring<TDefinition>(
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata,
        DateTime? startAfter,
        DateTime? endBefore,
        string? clusterId)
        => Recurring(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), expression, data, startAfter, endBefore, clusterId);

    public RecurringScheduleContext Recurring(
        JobDefinitionConfig config,
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null)
    {
        var recurring = NewRecurSchedule(clusterId, config, data, expression, startAfter, endBefore);
        var raw = recurring.ToModel();
        EnsureCanSave(clusterId, raw);
        SaveRecurringSchedule(raw);
        return RecurringScheduleConvertUtil.ToContext(recurring);
    }

    async Task<RecurringScheduleContext> IJobMasterSchedulerAdvanced.RecurringAsync<TDefinition>(
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata,
        DateTime? startAfter,
        DateTime? endBefore,
        string? clusterId)
        => await RecurringAsync(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), expression, data, startAfter, endBefore, clusterId);

    public async Task<RecurringScheduleContext> RecurringAsync(
        JobDefinitionConfig config,
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null)
    {
        var recurring = NewRecurSchedule(clusterId, config, data, expression, startAfter, endBefore);
        var raw = recurring.ToModel();
        EnsureCanSave(clusterId, raw);
        await SaveRecurringScheduleAsync(raw);
        return RecurringScheduleConvertUtil.ToContext(recurring);
    }

    RecurringScheduleContext IJobMasterSchedulerAdvanced.Recurring<TDefinition>(
        string expressionTypeId,
        string expression,
        IWriteableMessageData? data,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata,
        DateTime? startAfter,
        DateTime? endBefore,
        string? clusterId)
        => Recurring(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), expressionTypeId, expression, data, startAfter, endBefore, clusterId);

    public RecurringScheduleContext Recurring(
        JobDefinitionConfig config,
        string expressionTypeId,
        string expression,
        IWriteableMessageData? data = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null)
    {
        var compiled = RecurrenceExprCompiler.Compile(expressionTypeId, expression);
        var recurring = NewRecurSchedule(clusterId, config, data, compiled, startAfter, endBefore);
        var raw = recurring.ToModel();
        EnsureCanSave(clusterId, raw);
        SaveRecurringSchedule(raw);
        return RecurringScheduleConvertUtil.ToContext(recurring);
    }

    async Task<RecurringScheduleContext> IJobMasterSchedulerAdvanced.RecurringAsync<TDefinition>(
        string expressionTypeId,
        string expression,
        IWriteableMessageData? data,
        JobMasterPriority? priority,
        string? workerLane,
        TimeSpan? timeout,
        int? maxNumberOfRetries,
        IWritableMetadata? metadata,
        DateTime? startAfter,
        DateTime? endBefore,
        string? clusterId)
        => await RecurringAsync(ApplyOverrides(JobDefinitionConfigAttribute.GetConfig(typeof(TDefinition)), priority, workerLane, timeout, maxNumberOfRetries, metadata), expressionTypeId, expression, data, startAfter, endBefore, clusterId);

    public async Task<RecurringScheduleContext> RecurringAsync(
        JobDefinitionConfig config,
        string expressionTypeId,
        string expression,
        IWriteableMessageData? data = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null)
    {
        var compiled = RecurrenceExprCompiler.Compile(expressionTypeId, expression);
        var recurring = NewRecurSchedule(clusterId, config, data, compiled, startAfter, endBefore);
        var raw = recurring.ToModel();
        EnsureCanSave(clusterId, raw);
        await SaveRecurringScheduleAsync(raw);
        return RecurringScheduleConvertUtil.ToContext(recurring);
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
            metadata: metadata ?? config.Metadata);
    }

    private static string ResolveClusterId(string? clusterId)
    {
        if (clusterId != null)
        {
            return clusterId;
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
        clusterId = ResolveClusterId(clusterId);
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
        clusterId = ResolveClusterId(clusterId);
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
        clusterId = ResolveClusterId(clusterId);
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
        clusterId = ResolveClusterId(clusterId);
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
    
    

    private void EnsureCanSave(string? clusterId, RecurringScheduleRawModel recurringSchMd)
    {
        if (recurringSchMd.MaxNumberOfRetries > JobMasterConstants.MaxAllowedRetries)
        {
            throw new ArgumentException($"MaxNumberOfRetries must be less than or equal to {JobMasterConstants.MaxAllowedRetries}.");
        }

        EnsureCanSave(clusterId);
        var config = EnsureGetMasterClusterConfigurationService(clusterId).Get();
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

    private void EnsureCanSave(string? clusterId, JobRawModel job)
    {
        if (job.MaxNumberOfRetries > JobMasterConstants.MaxAllowedRetries)
        {
            throw new ArgumentException($"MaxNumberOfRetries must be less than or equal to {JobMasterConstants.MaxAllowedRetries}.");
        }

        EnsureCanSave(clusterId);
        var config = EnsureGetMasterClusterConfigurationService(clusterId).Get();
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
    
    
    private void SaveJob(JobRawModel jobRawModel)
    {
        var service = EnsureGetSchedulerClusterAware(jobRawModel.ClusterId);
        service.Schedule(jobRawModel);
    }

    private void SaveRecurringSchedule(RecurringScheduleRawModel recurringRawModel)
    {
        var service = EnsureGetSchedulerClusterAware(recurringRawModel.ClusterId);
        service.Schedule(recurringRawModel);
    }
    
    private async Task SaveJobAsync(JobRawModel jobRawModel)
    {
        var service = EnsureGetSchedulerClusterAware(jobRawModel.ClusterId);
        await service.ScheduleAsync(jobRawModel);
    }

    private async Task SaveRecurringScheduleAsync(RecurringScheduleRawModel recurringRawModel)
    {
        var service = EnsureGetSchedulerClusterAware(recurringRawModel.ClusterId);
        await service.ScheduleAsync(recurringRawModel);
    }
}
