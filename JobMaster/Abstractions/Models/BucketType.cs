namespace JobMaster.Abstractions.Models;

/// <summary>
/// Defines the type of a job execution bucket.
/// </summary>
public enum BucketType
{
    /// <summary>A normal distributed bucket managed by the cluster coordinator.</summary>
    Standard = 1,
    /// <summary>A temporary local bucket created when no standard bucket is available, to prevent job starvation.</summary>
    Fallback
}
