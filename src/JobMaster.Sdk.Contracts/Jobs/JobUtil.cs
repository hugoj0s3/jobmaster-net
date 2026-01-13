using JobMaster.Contracts.Models;
using JobMaster.Contracts.Models.Attributes;
using JobMaster.Sdk.Contracts.Models;

namespace JobMaster.Sdk.Contracts.Jobs;

public static class JobUtil
{
    public static string GetJobDefinitionId(Type jobHandlerType)
    {
        var jobDefinitionId =
            jobHandlerType.GetCustomAttributes(false).OfType<JobMasterDefinitionIdAttribute>().FirstOrDefault()?.JobDefinitionId ??
            jobHandlerType.FullName;
        
        if (string.IsNullOrEmpty(jobDefinitionId))
        {
            throw new InvalidOperationException($"JobDefinitionId was not resolved. " +
                                                $"try to add jobDefinitionIdAttribute on {jobHandlerType}.");
        }
        
        return jobDefinitionId;
    }
    
    public static TimeSpan GetTimeout(Type jobHandlerType, TimeSpan? timeout, ClusterConfigurationModel? masterConfig)
    {
        if (timeout is null)
        {
            var timeoutInSeconds = jobHandlerType.GetCustomAttributes(false)
                .OfType<JobMasterTimeoutAttribute>()
                .FirstOrDefault()?.TimeoutInSeconds;

            timeout = timeoutInSeconds.HasValue ? 
                System.TimeSpan.FromSeconds(timeoutInSeconds.Value) : 
                masterConfig?.DefaultJobTimeout ?? TimeSpan.FromMinutes(5);
        }
        else
        {
            timeout = timeout.Value;
        }
        return timeout.Value;
    }

    public static string? GetWorkerLane(Type jobHandlerType, string? workerLane)
    {
        return workerLane ?? jobHandlerType.GetCustomAttributes(false)
            .OfType<JobMasterWorkerLaneAttribute>()
            .FirstOrDefault()?.WorkerLane;
    }
    
    public static int GetMaxNumberOfRetries(Type jobHandlerType, int? maxNumberOfRetries, ClusterConfigurationModel? masterConfig)
    {
        var result = 3;
        if (maxNumberOfRetries is null)
        {
            result = jobHandlerType.GetCustomAttributes(false)
                .OfType<JobMasterMaxNumberOfRetriesAttribute>()
                .FirstOrDefault()?.MaxNumberOfRetries ?? masterConfig?.DefaultMaxOfRetryCount ?? 3;
        } 
        else
        {
            result = maxNumberOfRetries.Value;
        }

        if (result > 10)
        {
            throw new ArgumentException("MaxNumberOfRetries must be less than or equal to 10.");
        }
        
        return result;
    }
    
    public static JobMasterPriority GetJobMasterPriority(Type jobHandlerType, JobMasterPriority? priority)
    {
        if (priority is null)
        {
            priority = jobHandlerType.GetCustomAttributes(false)
                .OfType<JobMasterPriorityAttribute>()
                .FirstOrDefault()?.Priority ?? JobMasterPriority.Medium;
        }
        
        return priority.Value;
    }
    
    private static readonly IDictionary<string, Type> JobHandlerTypeMap = new Dictionary<string, Type>();
}