using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils.Extensions;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services.Master;

internal class MasterRecurringSchedulesService : JobMasterClusterAwareComponent, IMasterRecurringSchedulesService
{
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private IMasterRecurringSchedulesRepository masterRecurringSchedulesRepository = null!;
    private JobMasterLockKeys jobMasterLockKeys = null!;
    private readonly OperationLimiter operationLimiter;

    public MasterRecurringSchedulesService(
        IMasterDistributedLockerService masterDistributedLockerService,
        JobMasterClusterConnectionConfig clusterConnConfig,
        IMasterRecurringSchedulesRepository masterRecurringSchedulesRepository,
        IJobMasterRuntime runtime)
        : base(clusterConnConfig)
    {
        this.masterDistributedLockerService = masterDistributedLockerService;
        this.masterRecurringSchedulesRepository = masterRecurringSchedulesRepository;

        jobMasterLockKeys = new JobMasterLockKeys(clusterConnConfig.ClusterId);
        operationLimiter = runtime.GetOperationLimiterForCluster(clusterConnConfig.ClusterId);
    }

    public Task UpsertAsync(RecurringScheduleRawModel scheduleRaw)
    {
        return operationLimiter.ExecAsync(() => masterRecurringSchedulesRepository.UpsertAsync(scheduleRaw));
    }

    public void Upsert(RecurringScheduleRawModel scheduleRaw)
    {
        operationLimiter.Exec(() => { masterRecurringSchedulesRepository.Upsert(scheduleRaw); return true; });
    }

    public void UpsertStatic(StaticRecurringScheduleDefinition definition)
    {
        operationLimiter.Exec(() =>
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
        return operationLimiter.Exec(() => masterRecurringSchedulesRepository.Query(queryCriteria));
    }

    public void BulkUpdateStaticDefinitionLastEnsured(IList<string> staticDefinitionIds, DateTime ensuredAt)
    {
        if (staticDefinitionIds.IsNullOrEmpty())
        {
            return;
        }
        
        operationLimiter.Exec(() => { masterRecurringSchedulesRepository.BulkUpdateStaticDefinitionLastEnsuredByStaticIds(staticDefinitionIds, ensuredAt); return true; });
    }

    public Task<IList<RecurringScheduleRawModel>> QueryAsync(RecurringScheduleQueryCriteria queryCriteria)
    {
        return operationLimiter.ExecAsync(() => masterRecurringSchedulesRepository.QueryAsync(queryCriteria));
    }

    public async Task<IList<Guid>> QueryIdsAsync(RecurringScheduleQueryCriteria queryCriteria)
    {
        var rows = await operationLimiter.ExecAsync(() => masterRecurringSchedulesRepository.QueryAsync(queryCriteria));
        return rows.Select(x => x.Id).ToList();
    }

    public Task<IList<RecurringScheduleRawModel>> AcquireAndFetchAsync(RecurringScheduleQueryCriteria queryCriteria, int partitionLockId, DateTime expiresAtUtc)
    {
        return operationLimiter.ExecAsync(() => masterRecurringSchedulesRepository.AcquireAndFetchAsync(queryCriteria, partitionLockId, expiresAtUtc));
    }

    public Task<IList<RecurringScheduleRawModel>> AcquireAndFetchByIdsAsync(IList<Guid> ids, int partitionLockId, DateTime expiresAtUtc)
    {
        var criteria = new RecurringScheduleQueryCriteria
        {
            Ids = ids,
            Status = RecurringScheduleStatus.Active,
            IsLocked = false,
            CountLimit = ids.Count,
        };
        return operationLimiter.ExecAsync(() => masterRecurringSchedulesRepository.AcquireAndFetchAsync(criteria, partitionLockId, expiresAtUtc));
    }

    public Task<int> InactivateStaticDefinitionsOlderThanAsync(DateTime cutoff)
    {
        return operationLimiter.ExecAsync(() => masterRecurringSchedulesRepository.InactivateStaticDefinitionsOlderThanAsync(cutoff));
    }

    public long Count(RecurringScheduleQueryCriteria queryCriteria)
    {
        return operationLimiter.Exec(() => masterRecurringSchedulesRepository.Count(queryCriteria));
    }

    public RecurringScheduleRawModel? Get(Guid recurringScheduleId)
    {
        return operationLimiter.Exec(() => masterRecurringSchedulesRepository.Get(recurringScheduleId));
    }

    public Task<RecurringScheduleRawModel?> GetAsync(Guid recurringScheduleId)
    {
        return operationLimiter.ExecAsync(() => masterRecurringSchedulesRepository.GetAsync(recurringScheduleId));
    }
}
