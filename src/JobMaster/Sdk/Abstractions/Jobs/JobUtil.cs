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
                var timeoutInSeconds = jobHandlerType.GetCachedAttributes<JobMasterTimeoutAttribute>(inherit: true).FirstOrDefault()?.TimeoutInSeconds;

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
               ?? jobHandlerType.GetCachedAttributes<JobMasterWorkerLaneAttribute>(inherit: true).FirstOrDefault()?.WorkerLane;
    }

    /// <summary>
    /// Explicit → applied <see cref="JobDefinitionConfigAttribute"/> config → <see cref="JobMasterClusterIdAttribute"/>.
    /// Returns <c>null</c> when none is set; the caller applies its own default cluster.
    /// </summary>
    public static string? GetClusterId(Type jobHandlerType, string? clusterId)
    {
        return clusterId
               ?? JobDefinitionConfigAttribute.TryGetAppliedConfig(jobHandlerType)?.ClusterId
               ?? jobHandlerType.GetCachedAttributes<JobMasterClusterIdAttribute>(inherit: true).FirstOrDefault()?.ClusterId;
    }

    /// <summary>
    /// <see cref="JobMasterMetadataAttribute"/> values from the handler and its base classes. Walked from the
    /// top-most base down, so a derived class's key overrides the same key on a base class -- explicit rather than
    /// relying on reflection's (unspecified) inherited-attribute order. Duplicate keys on one class still throw.
    /// </summary>
    public static IDictionary<string, object?> GetHandlerMetadata(Type jobHandlerType)
    {
        var hierarchy = new Stack<Type>();
        for (var type = jobHandlerType; type != null && type != typeof(object); type = type.BaseType)
        {
            hierarchy.Push(type);
        }

        var result = new Dictionary<string, object?>();
        while (hierarchy.Count > 0)
        {
            var declared = hierarchy.Pop()
                .GetCachedAttributes<JobMasterMetadataAttribute>(inherit: false)
                .Select(attr => attr.ToKeyValuePair())
                .ToDictionary(x => x.Key, x => x.Value);

            foreach (var kv in declared)
            {
                result[kv.Key] = kv.Value;
            }
        }

        return result;
    }

    public static int GetMaxNumberOfRetries(Type jobHandlerType, int? maxNumberOfRetries, ClusterConfigurationModel? masterConfig)
    {
        var result = JobMasterDefaults.MaxRetryCount;
        if (maxNumberOfRetries is null)
        {
            result = JobDefinitionConfigAttribute.TryGetAppliedConfig(jobHandlerType)?.MaxNumberOfRetries
                     ?? jobHandlerType.GetCachedAttributes<JobMasterMaxNumberOfRetriesAttribute>(inherit: true).FirstOrDefault()?.MaxNumberOfRetries
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
                        ?? jobHandlerType.GetCachedAttributes<JobMasterPriorityAttribute>(inherit: true).FirstOrDefault()?.Priority
                        ?? JobMasterPriority.Medium;
        }

        return priority.Value;
    }

    // The priority a recurring schedule with no priority of its own resolves to on each run; null when no handler
    // for the definition id is in this process (e.g. a publisher-only app), since the worker resolves it then.
    public static JobMasterPriority? GetHandlerPriority(string jobDefinitionId)
    {
        var jobHandlerType = JobMasterDefinitionIdAttribute.GetJobHandlerTypeFromId(jobDefinitionId);
        return jobHandlerType is null ? null : GetJobMasterPriority(jobHandlerType, priority: null);
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