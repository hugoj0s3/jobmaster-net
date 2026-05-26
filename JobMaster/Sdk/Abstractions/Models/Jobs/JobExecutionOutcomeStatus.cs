namespace JobMaster.Sdk.Abstractions.Models.Jobs;

/// <summary>
/// Records the final outcome of a single job execution attempt, stored in the execution history.
/// Values start at <c>1</c> so that <c>0</c> can be used as an uninitialized sentinel
/// (a <c>JobExecution</c> with outcome <c>0</c> has not been finalized yet).
/// </summary>
public enum JobExecutionOutcomeStatus
{
    /// <summary>The handler ran to completion without throwing an exception.</summary>
    Succeeded = 1,
    /// <summary>The handler did not complete successfully — covers unhandled exceptions, timeouts, and explicit cancellation.</summary>
    Failed,
}
