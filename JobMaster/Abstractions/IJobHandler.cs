using JobMaster.Abstractions.Models;

namespace JobMaster.Abstractions;

/// <summary>
/// Implement this interface to define a job handler.
/// Each implementation represents one job type; the runtime invokes
/// <see cref="HandleAsync"/> when a scheduled job of that type is due.
/// Register handlers with the cluster via the IoC configuration.
/// </summary>
public interface IJobHandler
{
    /// <summary>
    /// Executes the job. Called by the runtime when the job is due.
    /// </summary>
    /// <param name="job">Context object containing the job's payload, metadata, and scheduling information.</param>
    Task HandleAsync(JobContext job);
}
