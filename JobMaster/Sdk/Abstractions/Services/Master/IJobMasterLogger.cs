using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Logs;

namespace JobMaster.Sdk.Abstractions.Services.Master;

internal interface IJobMasterLogger : IJobMasterClusterAwareService
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