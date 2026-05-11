using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils.Extensions;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Services.Master;

internal class MasterRecurringSchedulesService : JobMasterClusterAwareComponent, IMasterRecurringSchedulesService
{
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private IMasterRecurringSchedulesRepository masterRecurringSchedulesRepository = null!;
    private JobMasterLockKeys jobMasterLockKeys = null!;
    private readonly OperationThrottler operationThrottler;
    private readonly IJobMasterLogger logger;
    private readonly IKnownExceptionIdentifier exceptionIdentifier;
    
    private readonly OperationThrottler acquireOperationThrottler = new(1, 5000);
    private readonly RetryDeadlockPolicy retryDeadlockPolicy;

    public MasterRecurringSchedulesService(
        IMasterDistributedLockerService masterDistributedLockerService,
        JobMasterClusterConnectionConfig clusterConnConfig,
        IMasterRecurringSchedulesRepository masterRecurringSchedulesRepository,
        IJobMasterRuntime runtime,
        IJobMasterLogger logger,
        IKnownExceptionIdentifier exceptionIdentifier)
        : base(clusterConnConfig)
    {
        this.masterDistributedLockerService = masterDistributedLockerService;
        this.masterRecurringSchedulesRepository = masterRecurringSchedulesRepository;
        this.logger = logger;
        this.exceptionIdentifier = exceptionIdentifier;

        jobMasterLockKeys = new JobMasterLockKeys(clusterConnConfig.ClusterId);
        operationThrottler = runtime.GetOperationLimiterForCluster(clusterConnConfig.ClusterId);
        retryDeadlockPolicy = new RetryDeadlockPolicy(this.exceptionIdentifier, TimeSpan.FromMilliseconds(250), 3);
    }

    public async Task UpsertAsync(RecurringScheduleRawModel scheduleRaw)
    {
        try
        {
            await operationThrottler.ExecAsync(() => masterRecurringSchedulesRepository.UpsertAsync(scheduleRaw));
        }
        catch (JobMasterVersionConflictException e)
        {
            this.logger.Error("RecurringSchedule version conflict", JobMasterLogSubjectType.RecurringSchedule, scheduleRaw.Id, e);
            throw;
        }
    }

    public void Upsert(RecurringScheduleRawModel scheduleRaw)
    {
        try
        {
            operationThrottler.Exec(() => { masterRecurringSchedulesRepository.Upsert(scheduleRaw); return true; });
        }
        catch (JobMasterVersionConflictException e)
        {
            this.logger.Error("RecurringSchedule version conflict", JobMasterLogSubjectType.RecurringSchedule, scheduleRaw.Id, e);
            throw;
        }
    }

    public void UpsertStatic(StaticRecurringScheduleDefinition definition)
    {
        operationThrottler.Exec(() =>
        {
            var rawModel = masterRecurringSchedulesRepository.GetByStaticId(definition.Id);
            if (rawModel == null)
            {
                var recurringSchedule = RecurringSchedule.New(
                    ClusterConnConfig.ClusterId,
                    definition.JobDefinitionId,
                    MessageData.Empty,
                    definition.CompiledExpr,
                    definition.Priority,
                    definition.Timeout,
                    definition.MaxNumberOfRetries,
                    Metadata.Empty,
                    RecurringScheduleType.Static,
                    definition.Id,
                    definition.StartAfter,
                    definition.EndBefore,
                    definition.WorkerLane
                ).ToModel();
                
                recurringSchedule.UpdateStaticFromDefinition(definition);
                masterRecurringSchedulesRepository.Add(recurringSchedule);
            }
            else
            {
                rawModel.UpdateStaticFromDefinition(definition);
                masterRecurringSchedulesRepository.Upsert(rawModel);
            }
            return true;
        });
    }

    public IList<RecurringScheduleRawModel> Query(RecurringScheduleQueryCriteria queryCriteria)
    {
        return operationThrottler.Exec(() => masterRecurringSchedulesRepository.Query(queryCriteria));
    }

    public void BulkUpdateStaticDefinitionLastEnsured(IList<string> staticDefinitionIds, DateTime ensuredAt)
    {
        if (staticDefinitionIds.IsNullOrEmpty())
        {
            return;
        }
        
        operationThrottler.Exec(() => { masterRecurringSchedulesRepository.BulkUpdateStaticDefinitionLastEnsuredByStaticIds(staticDefinitionIds, ensuredAt); return true; });
    }

    public Task<IList<RecurringScheduleRawModel>> QueryAsync(RecurringScheduleQueryCriteria queryCriteria)
    {
        return operationThrottler.ExecAsync(() => masterRecurringSchedulesRepository.QueryAsync(queryCriteria));
    }

    public async Task<IList<Guid>> QueryIdsAsync(RecurringScheduleQueryCriteria queryCriteria)
    {
        var rows = await operationThrottler.ExecAsync(() => masterRecurringSchedulesRepository.QueryAsync(queryCriteria));
        return rows.Select(x => x.Id).ToList();
    }
    
    public Task<IList<RecurringScheduleRawModel>> AcquireAndFetchAsync(RecurringScheduleQueryCriteria queryCriteria, DateTime expiresAtUtc)
    {
        var partitionLockId = JobMasterRandomUtil.NewGuid7();
        return retryDeadlockPolicy.ExecAsync(() => acquireOperationThrottler.ExecAsync(() => masterRecurringSchedulesRepository.AcquireAndFetchAsync(queryCriteria, partitionLockId, expiresAtUtc)));
    }

    public Task<IList<RecurringScheduleRawModel>> AcquireAndFetchByIdsAsync(IList<Guid> ids, DateTime expiresAtUtc)
    {
        var partitionLockId = JobMasterRandomUtil.NewGuid7();
        var criteria = new RecurringScheduleQueryCriteria
        {
            Ids = ids,
            Status = RecurringScheduleStatus.Active,
            IsLocked = false,
            CountLimit = ids.Count,
        };
        return retryDeadlockPolicy.ExecAsync(() => acquireOperationThrottler.ExecAsync(() => masterRecurringSchedulesRepository.AcquireAndFetchAsync(criteria, partitionLockId, expiresAtUtc)));
    }

    public Task<int> InactivateStaticDefinitionsOlderThanAsync(DateTime cutoff)
    {
        return operationThrottler.ExecAsync(() => masterRecurringSchedulesRepository.InactivateStaticDefinitionsOlderThanAsync(cutoff));
    }

    public long Count(RecurringScheduleQueryCriteria queryCriteria)
    {
        return operationThrottler.Exec(() => masterRecurringSchedulesRepository.Count(queryCriteria));
    }

    public RecurringScheduleRawModel? Get(Guid recurringScheduleId)
    {
        return operationThrottler.Exec(() => masterRecurringSchedulesRepository.Get(recurringScheduleId));
    }

    public Task<RecurringScheduleRawModel?> GetAsync(Guid recurringScheduleId)
    {
        return operationThrottler.ExecAsync(() => masterRecurringSchedulesRepository.GetAsync(recurringScheduleId));
    }
}
