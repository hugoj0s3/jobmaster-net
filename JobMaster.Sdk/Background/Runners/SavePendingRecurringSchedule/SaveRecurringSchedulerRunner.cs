using JobMaster.Contracts.Models;
using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Background.Runners;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Models.RecurringSchedules;
using JobMaster.Sdk.Contracts.Services;
using JobMaster.Sdk.Contracts.Services.Agent;
using JobMaster.Sdk.Contracts.Services.Master;

namespace JobMaster.Sdk.Background.Runners.SavePendingRecurringSchedule;

public abstract class SaveRecurringSchedulerRunner : BucketAwareRunner, ISaveRecurringSchedulerRunner
{
    protected readonly IRecurringSchedulePlanner RecurringSchedulePlanner;
    protected readonly IMasterRecurringSchedulesService masterRecurringSchedulesService;
    protected readonly IMasterDistributedLockerService distributedLockerService;
    protected readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    protected readonly IMasterBucketsService masterBucketsService;
    protected readonly JobMasterLockKeys lockKeys;
    private IWorkerClusterOperations workerClusterOperations;
    
    public SaveRecurringSchedulerRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
        RecurringSchedulePlanner = backgroundAgentWorker.GetClusterAwareService<IRecurringSchedulePlanner>();
        distributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        masterRecurringSchedulesService = backgroundAgentWorker.GetClusterAwareService<IMasterRecurringSchedulesService>();
        agentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        workerClusterOperations = backgroundAgentWorker.GetClusterAwareService<IWorkerClusterOperations>();
        
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
    }
    
    
    protected async Task ScheduleNextJobs(RecurringScheduleRawModel recurringScheduleRawModel)
    {
        if (this.distributedLockerService.IsLocked(lockKeys.RecurringScheduleCancellingLock(recurringScheduleRawModel.Id)))
        {
            BackgroundAgentWorker.WorkerClusterOperations.CancelRecurringSchedule(recurringScheduleRawModel.Id);
            logger.Debug("Recurring schedule cancelled", JobMasterLogSubjectType.RecurringSchedule, recurringScheduleRawModel.Id);
            return;
        }

        logger.Debug("Scheduling next jobs", JobMasterLogSubjectType.RecurringSchedule, recurringScheduleRawModel.Id);
        await RecurringSchedulePlanner.ScheduleNextJobsAsync(recurringScheduleRawModel);
    }
    
    protected async Task SaveRecurringScheduleAsync(RecurringScheduleRawModel recurringScheduleRawModel)
    {
        if (recurringScheduleRawModel.Status == RecurringScheduleStatus.PendingSave)
        {
            recurringScheduleRawModel.Active();
        }
        
        await workerClusterOperations.ExecWithRetryAsync(o => o.Upsert(recurringScheduleRawModel));
    }
    

    public void DefineBucketId(string bucketId)
    {
        if (string.IsNullOrEmpty(bucketId))
            throw new ArgumentNullException(nameof(bucketId));
        
        if (!string.IsNullOrEmpty(BucketId))
            throw new InvalidOperationException("BucketId is already defined.");
        
        this.BucketId = bucketId;
    }
}