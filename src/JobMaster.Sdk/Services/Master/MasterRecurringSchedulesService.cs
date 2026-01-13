using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Models.RecurringSchedules;
using JobMaster.Sdk.Contracts.Repositories.Master;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Ioc.Markups;
using System.Linq;
using JobMaster.Contracts.Extensions;
using JobMaster.Contracts.Models;
using JobMaster.Contracts.StaticRecurringSchedules;
using JobMaster.Sdk.Contracts.Jobs;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts;

namespace JobMaster.Sdk.Services.Master;

public class MasterRecurringSchedulesService : JobMasterClusterAwareComponent, IMasterRecurringSchedulesService
{
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private IMasterRecurringSchedulesRepository masterRecurringSchedulesRepository = null!;
    private JobMasterLockKeys jobMasterLockKeys = null!;
    private readonly OperationThrottler operationThrottler;

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
        operationThrottler = runtime.GetOperationThrottlerForCluster(clusterConnConfig.ClusterId);
    }

    public async Task UpsertAsync(RecurringScheduleRawModel scheduleRaw)
    {
        await operationThrottler.ExecAsync(async () =>
        {
            var entity = await masterRecurringSchedulesRepository.GetAsync(scheduleRaw.Id);
            if (entity == null)
            {
                await masterRecurringSchedulesRepository.AddAsync(scheduleRaw);
            }

            await masterRecurringSchedulesRepository.UpdateAsync(scheduleRaw);
        });
    }

    public void Upsert(RecurringScheduleRawModel scheduleRaw)
    {
        operationThrottler.Exec(() =>
        {
            var entity = masterRecurringSchedulesRepository.Get(scheduleRaw.Id);
            if (entity is null)
            {
                masterRecurringSchedulesRepository.Add(scheduleRaw);
            }

            masterRecurringSchedulesRepository.Update(scheduleRaw);
            return true;
        });
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
                masterRecurringSchedulesRepository.Update(rawModel);
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

    public bool BulkUpdatePartitionLockId(IList<Guid> recurringScheduleIds, int lockId, DateTime expiresAt)
    {
        if (recurringScheduleIds.IsNullOrEmpty())
        {
            return false;
        }
        
        return operationThrottler.Exec(() => masterRecurringSchedulesRepository.BulkUpdatePartitionLockId(recurringScheduleIds, lockId, expiresAt));
    }
}