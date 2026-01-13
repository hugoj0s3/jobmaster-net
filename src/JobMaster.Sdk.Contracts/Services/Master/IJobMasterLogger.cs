using JobMaster.Sdk.Contracts.Ioc.Markups;
using JobMaster.Sdk.Contracts.Models;
using JobMaster.Sdk.Contracts.Models.Logs;

namespace JobMaster.Sdk.Contracts.Services.Master;

public interface IJobMasterLogger : IJobMasterClusterAwareService
{
    void Log(
        JobMasterLogLevel level,
        string message,
        JobMasterLogSubjectType? subjectType = null,
        string? subjectId = null,
        Exception? exception = null,
        string? sourceMember = null,
        string? sourceFile = null,
        int? sourceLine = null);
    
    Task<List<LogItem>> QueryAsync(LogItemQueryCriteria criteria);
}