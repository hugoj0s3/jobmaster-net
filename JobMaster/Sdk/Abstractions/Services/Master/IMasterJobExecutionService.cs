using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Sdk.Abstractions.Services.Master;

internal interface IMasterJobExecutionService : IJobMasterClusterAwareService
{
    Task SaveAsync(JobExecution jobExecution);
    void Save(JobExecution jobExecution);
    Task<IList<JobExecution>> QueryAsync(Guid jobId);
}