using JobMaster.Abstractions.Models;

namespace JobMaster.Abstractions;

/// <summary>
/// Encapsulates the identity and scheduling configuration of a job definition, independent of any
/// handler type. Use with <see cref="IJobMasterSchedulerAdvanced"/> to schedule a job from a publisher
/// that only knows the definition, not the handler that will eventually process it.
/// </summary>
public class JobDefinitionConfig
{
    /// <param name="jobDefinitionId">
    /// Stable, human-readable ID identifying the job definition. Required — unlike
    /// <see cref="Models.Attributes.JobMasterDefinitionIdAttribute"/>'s optional ID, this has no
    /// type-name fallback, since a definition used for publisher/consumer separation should not
    /// depend on a type name that only the consumer even sees.
    /// </param>
    public JobDefinitionConfig(
        string jobDefinitionId,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        string? workerLane = null,
        IWritableMetadata? metadata = null)
    {
        if (string.IsNullOrWhiteSpace(jobDefinitionId))
            throw new ArgumentException("JobDefinitionId is required.", nameof(jobDefinitionId));

        JobDefinitionId = jobDefinitionId;
        Priority = priority;
        Timeout = timeout;
        MaxNumberOfRetries = maxNumberOfRetries;
        WorkerLane = workerLane;
        Metadata = metadata;
    }

    /// <summary>Stable, human-readable ID identifying the job definition.</summary>
    public string JobDefinitionId { get; }

    /// <summary>Execution priority. Falls back to <see cref="JobMasterPriority.Medium"/> if unset.</summary>
    public JobMasterPriority? Priority { get; }

    /// <summary>Maximum execution time. Falls back to the cluster default if unset.</summary>
    public TimeSpan? Timeout { get; }

    /// <summary>Max retries on failure. Falls back to the cluster default if unset.</summary>
    public int? MaxNumberOfRetries { get; }

    /// <summary>Routes the job to a dedicated worker lane. Falls back to the default lane if unset.</summary>
    public string? WorkerLane { get; }

    /// <summary>Optional key-value metadata passed to the handler.</summary>
    public IWritableMetadata? Metadata { get; }
}
