using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services.Master;

internal class MasterJobExecutionService : JobMasterClusterAwareComponent, IMasterJobExecutionService
{
    private readonly IMasterGenericRecordRepository masterGenericRecordRepository;

    public MasterJobExecutionService(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IMasterGenericRecordRepository masterGenericRecordRepository) : base(clusterConnConfig)
    {
        this.masterGenericRecordRepository = masterGenericRecordRepository;
    }

    public async Task SaveAsync(JobExecution jobExecution)
    {
        var record = GenericRecordEntry.Create(
            ClusterConnConfig.ClusterId, 
            MasterGenericRecordGroupIds.JobExecution,
            jobExecution.Id,
            jobExecution);

        await masterGenericRecordRepository.UpsertAsync(record);
    }

    public void Save(JobExecution jobExecution)
    {
        var record = GenericRecordEntry.Create(
            ClusterConnConfig.ClusterId, 
            MasterGenericRecordGroupIds.JobExecution,
            jobExecution.Id,
            jobExecution);
        
        masterGenericRecordRepository.Upsert(record);
    }


    public async Task<IList<JobExecution>> QueryAsync(Guid jobId)
    {
        var genericFilter = new GenericRecordValueFilter()
        {
            Key = nameof(JobExecution.JobId),
            Operation = GenericFilterOperation.Eq,
            Value = jobId.ToString(),
        };

        var criteria = new GenericRecordQueryCriteria()
        {
            Filters = new List<GenericRecordValueFilter>()
            {
                genericFilter
            },
            
            ReadIsolationLevel = ReadIsolationLevel.FastSync,
            OrderBy = GenericRecordQueryOrderByTypeId.CreatedAtDesc,
        };
        
        var genericQueryResult = 
            await masterGenericRecordRepository.QueryAsync(MasterGenericRecordGroupIds.JobExecution, criteria);
        
        return genericQueryResult.Select(x => x.ToObject<JobExecution>()).ToList();
    }
}