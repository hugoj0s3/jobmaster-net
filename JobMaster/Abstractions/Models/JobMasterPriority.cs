namespace JobMaster.Abstractions.Models;

/// <summary>Job execution priority. Higher-priority jobs are processed before lower-priority ones within the same worker.</summary>
public enum JobMasterPriority
{
    /// <summary>Lowest priority. Processed last when the worker is under load.</summary>
    VeryLow = 1,

    /// <summary>Low priority.</summary>
    Low = 2,

    /// <summary>Standard priority (recommended default for most jobs).</summary>
    Medium = 3,

    /// <summary>High priority. Processed before Medium and below.</summary>
    High = 4,

    /// <summary>Highest priority. Processed first, ahead of all other levels.</summary>
    Critical = 5
}
