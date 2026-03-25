using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Serialization;
using JobMaster.Sdk.Abstractions.Services;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Repositories;
using NATS.Client.JetStream;

namespace JobMaster.NatsJetStream.Background;

internal class NetsJetStreamSaveRecurringScheduleRunner : NatsJetStreamRunnerBase<RecurringScheduleRawModel>, ISaveRecurringSchedulerRunner
{
    private readonly IMasterRecurringSchedulesService masterRecurringSchedulesService;
    private readonly IRecurringSchedulePlanner recurringSchedulePlanner;
    private readonly IWorkerClusterOperations workerClusterOperations;
    private readonly IMasterDistributedLockerService distributedLockerService;
    private readonly JobMasterLockKeys lockKeys;
    
    public NetsJetStreamSaveRecurringScheduleRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
        masterRecurringSchedulesService = BackgroundAgentWorker.GetClusterAwareService<IMasterRecurringSchedulesService>();
        workerClusterOperations = backgroundAgentWorker.GetClusterAwareService<IWorkerClusterOperations>();
        recurringSchedulePlanner = backgroundAgentWorker.GetClusterAwareService<IRecurringSchedulePlanner>();
        distributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
    }

    protected override string GetFullBucketAddressId(string bucketId) => FullBucketAddressIdsUtil.GetRecurringScheduleSavePendingBucketAddress(bucketId);
    protected override bool LostRisk() => true;
    protected override string GetRunnerDescription() => "SaveRecurringSchedule";

    protected override IReadOnlyCollection<BucketStatus> ValidBucketStatuses() => new[] { BucketStatus.Active, BucketStatus.Completing };

    protected override RecurringScheduleRawModel Deserialize(string json)
    {
        return InternalJobMasterSerializer.Deserialize<RecurringScheduleRawModel>(json);
    }

    protected override async Task ProcessPayloadAsync(RecurringScheduleRawModel payload, MsgAckGuard ackGuard)
    {
        try
        {
            await workerClusterOperations.ExecWithRetryAsync(o => o.Upsert(payload));
        }
        catch (Exception e)
        {
            this.logger.Error($"{GetRunnerDescription()} - Failed to save recurring schedule", JobMasterLogSubjectType.RecurringSchedule, payload.Id, e);
            throw;
        }

        await ScheduleNextJobsAsync(payload);
    }
    
    private async Task ScheduleNextJobsAsync(RecurringScheduleRawModel payload)
    {
        try
        {
            if (this.distributedLockerService.IsLocked(lockKeys.RecurringScheduleCancellingLock(payload.Id)))
            {
                BackgroundAgentWorker.WorkerClusterOperations.CancelRecurringSchedule(payload.Id);
                logger.Debug("Recurring schedule cancelled", JobMasterLogSubjectType.RecurringSchedule, payload.Id);
                return;
            }
            
            logger.Debug("Scheduling next jobs", JobMasterLogSubjectType.RecurringSchedule, payload.Id);
            await recurringSchedulePlanner.ScheduleNextJobsAsync(payload, byPassStatusValidation: true);
        }
        catch (Exception e)
        {
            this.logger.Error($"{GetRunnerDescription()} - Failed to schedule next jobs after save", JobMasterLogSubjectType.RecurringSchedule, payload.Id, e);
        }
        finally
        {
            if (payload.Status == RecurringScheduleStatus.PendingSave)
            {
                payload.Active();
            }
            
            await workerClusterOperations.ExecWithRetryAsync(o => o.Upsert(payload));
        }
    }

    protected override async Task<bool> ShouldAckAfterLockAsync(RecurringScheduleRawModel payload, CancellationToken ct)
    {
        var exists = await masterRecurringSchedulesService.GetAsync(payload.Id);
        return exists is not null;
    }
}