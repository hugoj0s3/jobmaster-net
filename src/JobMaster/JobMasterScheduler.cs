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

public class JobMasterScheduler : IJobMasterScheduler
{
    private static IJobMasterRuntime JobMasterRuntime => JobMasterRuntimeSingleton.Instance;
    
    private JobMasterScheduler()
    {
    }
    
    public static IJobMasterScheduler Instance { get; } = new JobMasterScheduler();
    
    public JobContext OnceNow<T>(
        IWriteableMessageData? msgData = null, 
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null, 
        int? maxNumberOfRetries = null, 
        IWritableMetadata? metadata = null, 
        string? clusterId = null) where T : IJobHandler
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
        string? clusterId = null) where T : IJobHandler
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
        string? clusterId = null) where T : IJobHandler
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
        string? clusterId = null) where T : IJobHandler
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
        string? clusterId = null) where T : IJobHandler
    {
        var job = NewJob<T>(clusterId, msgData, priority, timeout, maxNumberOfRetries, metadata, scheduledAt, workerLane);
        var jobRawModel = job.ToModel();
        EnsureCanSave(clusterId, jobRawModel);
        
        await SaveJobAsync(jobRawModel);
        return JobConvertUtil.ToJobContext(job);
    }

    public async Task<JobContext> OnceAfterAsync<T>(TimeSpan after, IWriteableMessageData? msgData = null, JobMasterPriority? priority = null, string? workerLane = null, TimeSpan? timeout = null, int? maxNumberOfRetries = null, IWritableMetadata? metadata = null,
        string? clusterId = null) where T : IJobHandler
    {
        var scheduledAt = DateTime.UtcNow.Add(after);
        var job = NewJob<T>(clusterId, msgData, priority, timeout, maxNumberOfRetries, metadata, scheduledAt, workerLane);
        var jobRawModel = job.ToModel();
        EnsureCanSave(clusterId, jobRawModel);
        
        await SaveJobAsync(jobRawModel);
        return JobConvertUtil.ToJobContext(job);
    }

    public RecurringScheduleContext Recurring<T>(IRecurrenceCompiledExpr expression, IWriteableMessageData? data = null, JobMasterPriority? priority = null, string? workerLane = null, TimeSpan? timeout = null, int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null, DateTime? startAfter = null, DateTime? endBefore = null, string? clusterId = null) where T : IJobHandler
    {
        var recurring = NewRecurSchedule<T>(clusterId, data, expression, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore, workerLane);
        var raw = recurring.ToModel();
        EnsureCanSave(clusterId, raw);
        SaveRecurringSchedule(raw);
        return RecurringScheduleConvertUtil.ToContext(recurring);
    }

    public async Task<RecurringScheduleContext> RecurringAsync<T>(IRecurrenceCompiledExpr expression, IWriteableMessageData? data = null, JobMasterPriority? priority = null,  string? workerLane = null, TimeSpan? timeout = null, int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null, DateTime? startAfter = null, DateTime? endBefore = null, string? clusterId = null) where T : IJobHandler
    {
        var recurring = NewRecurSchedule<T>(clusterId, data, expression, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore, workerLane);
        var raw = recurring.ToModel();
        EnsureCanSave(clusterId, raw);
        await SaveRecurringScheduleAsync(raw);
        
        return RecurringScheduleConvertUtil.ToContext(recurring);
    }

    public RecurringScheduleContext Recurring<T>(string expressionTypeId, string expression, IWriteableMessageData? data = null, JobMasterPriority? priority = null, string? workerLane = null, TimeSpan? timeout = null, int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null, DateTime? startAfter = null, DateTime? endBefore = null, string? clusterId = null) where T : IJobHandler
    {
        var compiled = RecurrenceExprCompiler.Compile(expressionTypeId, expression);
        var recurring = NewRecurSchedule<T>(clusterId, data, compiled, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore, workerLane);
        var raw = recurring.ToModel();
        EnsureCanSave(clusterId, raw);
        SaveRecurringSchedule(raw);
        return RecurringScheduleConvertUtil.ToContext(recurring);
    }

    public async Task<RecurringScheduleContext> RecurringAsync<T>(string expressionTypeId, string expression, IWriteableMessageData? data = null, JobMasterPriority? priority = null, string? workerLane = null,  TimeSpan? timeout = null, int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null, DateTime? startAfter = null, DateTime? endBefore = null, string? clusterId = null) where T : IJobHandler
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
        string? workerLane) where T : IJobHandler
    {
        if (clusterId == null)
        {
            if (JobMasterClusterConnectionConfig.Default == null)
            {
                throw new KeyNotFoundException("Default cluster config not found");
            }
            
            clusterId = JobMasterClusterConnectionConfig.Default.ClusterId;
        }
        
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
        string? workerLane) where T : IJobHandler
    {
        if (clusterId == null)
        {
            if (JobMasterClusterConnectionConfig.Default == null)
            {
                throw new KeyNotFoundException("Default cluster config not found");
            }
            
            clusterId = JobMasterClusterConnectionConfig.Default.ClusterId;
        }
        
        var clusterConfiguration = EnsureGetMasterClusterConfigurationService(clusterId).Get();
        return Job.New<T>(
            clusterId,
            data,
            scheduledAt,
            priority: priority,
            timeout: timeout,
            maxNumberOfRetries: maxNumberOfRetries,
            writableMetadata: writableMetadata,
            scheduledType: JobSchedulingSourceType.Once,
            masterConfig: clusterConfiguration,
            workerLane: workerLane);
    }
    
    

    private void EnsureCanSave(string? clusterId, RecurringScheduleRawModel recurringSchMd)
    {
        EnsureCanSave(clusterId);
        var config = EnsureGetMasterClusterConfigurationService(clusterId).Get();
        if (config == null)
            throw new KeyNotFoundException("Cluster config not found");
    }

    private void EnsureCanSave(string? clusterId, JobRawModel job)
    {
        EnsureCanSave(clusterId);
        var config = EnsureGetMasterClusterConfigurationService(clusterId).Get();
        if (config == null)
            throw new KeyNotFoundException("Cluster config not found");
    }

    private void EnsureCanSave(string? cluserId)
    {
        if (JobMasterRuntime == null || !JobMasterRuntime.Started)
            throw new InvalidOperationException("JobMasterRuntime is not initialized");
        
        if (cluserId == null)
        {
            if (JobMasterClusterConnectionConfig.Default == null)
            {
                throw new KeyNotFoundException("Default cluster config not found");
            }
            
            cluserId = JobMasterClusterConnectionConfig.Default.ClusterId;
        }
        
        var config = EnsureGetMasterClusterConfigurationService(cluserId).Get();
        if (config == null)
            throw new KeyNotFoundException("Cluster config not found");
        
        EnsureGetSchedulerClusterAware(cluserId);
    }
    
    private IJobMasterSchedulerClusterAware EnsureGetSchedulerClusterAware(string? clusterId)
    {
        if (JobMasterRuntime == null || !JobMasterRuntime.Started)
            throw new InvalidOperationException("JobMasterRuntime is not initialized");
        
        if (clusterId == null)
        {
            if (JobMasterClusterConnectionConfig.Default == null)
            {
                throw new KeyNotFoundException("Default cluster config not found");
            }
            
            clusterId = JobMasterClusterConnectionConfig.Default.ClusterId;
        }

        var factory = JobMasterClusterAwareComponentFactories.GetFactory(clusterId);

        return factory.GetComponent<IJobMasterSchedulerClusterAware>();
    }
    
    private IMasterClusterConfigurationService EnsureGetMasterClusterConfigurationService(string? clusterId)
    {
        if (clusterId == null)
        {
            if (JobMasterClusterConnectionConfig.Default == null)
            {
                throw new KeyNotFoundException("Default cluster config not found");
            }
            
            clusterId = JobMasterClusterConnectionConfig.Default.ClusterId;
        }
        
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
