using System.Reflection;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Abstractions.Jobs;

internal static class JobUtil
{
    public static string GetJobDefinitionId(Type jobHandlerType)
    {
        var jobDefinitionId = JobMasterDefinitionIdAttribute.GetJobDefinitionId(jobHandlerType);

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
            timeout = JobDefinitionConfigAttribute.TryGetAppliedConfig(jobHandlerType)?.Timeout;

            if (timeout is null)
            {
                var timeoutInSeconds = jobHandlerType.GetCachedAttributes<JobMasterTimeoutAttribute>(inherit: false).FirstOrDefault()?.TimeoutInSeconds;

                timeout = timeoutInSeconds.HasValue ?
                    System.TimeSpan.FromSeconds(timeoutInSeconds.Value) :
                    masterConfig?.DefaultJobTimeout ?? JobMasterDefaults.DefaultJobTimeout;
            }
        }
        else
        {
            timeout = timeout.Value;
        }
        return timeout.Value;
    }

    public static string? GetWorkerLane(Type jobHandlerType, string? workerLane)
    {
        return workerLane
               ?? JobDefinitionConfigAttribute.TryGetAppliedConfig(jobHandlerType)?.WorkerLane
               ?? jobHandlerType.GetCachedAttributes<JobMasterWorkerLaneAttribute>(inherit: false).FirstOrDefault()?.WorkerLane;
    }

    /// <summary>
    /// Explicit → applied <see cref="JobDefinitionConfigAttribute"/> config → <see cref="JobMasterClusterIdAttribute"/>.
    /// Returns <c>null</c> when none is set; the caller applies its own default cluster.
    /// </summary>
    public static string? GetClusterId(Type jobHandlerType, string? clusterId)
    {
        return clusterId
               ?? JobDefinitionConfigAttribute.TryGetAppliedConfig(jobHandlerType)?.ClusterId
               ?? jobHandlerType.GetCachedAttributes<JobMasterClusterIdAttribute>(inherit: false).FirstOrDefault()?.ClusterId;
    }

    public static int GetMaxNumberOfRetries(Type jobHandlerType, int? maxNumberOfRetries, ClusterConfigurationModel? masterConfig)
    {
        var result = JobMasterDefaults.MaxRetryCount;
        if (maxNumberOfRetries is null)
        {
            result = JobDefinitionConfigAttribute.TryGetAppliedConfig(jobHandlerType)?.MaxNumberOfRetries
                     ?? jobHandlerType.GetCachedAttributes<JobMasterMaxNumberOfRetriesAttribute>(inherit: false).FirstOrDefault()?.MaxNumberOfRetries
                     ?? masterConfig?.DefaultMaxOfRetryCount ?? JobMasterDefaults.MaxRetryCount;
        }
        else
        {
            result = maxNumberOfRetries.Value;
        }

        return ValidateMaxNumberOfRetries(result);
    }

    public static JobMasterPriority GetJobMasterPriority(Type jobHandlerType, JobMasterPriority? priority)
    {
        if (priority is null)
        {
            priority = JobDefinitionConfigAttribute.TryGetAppliedConfig(jobHandlerType)?.Priority
                        ?? jobHandlerType.GetCachedAttributes<JobMasterPriorityAttribute>(inherit: false).FirstOrDefault()?.Priority
                        ?? JobMasterPriority.Medium;
        }

        return priority.Value;
    }

    private static readonly IDictionary<string, Type> JobHandlerTypeMap = new Dictionary<string, Type>();

    public static int ValidateMaxNumberOfRetries(int maxNumberOfRetries)
    {
        if (maxNumberOfRetries > 10)
        {
            throw new ArgumentException("MaxNumberOfRetries must be less than or equal to 10.");
        }

        return maxNumberOfRetries;
    }
}