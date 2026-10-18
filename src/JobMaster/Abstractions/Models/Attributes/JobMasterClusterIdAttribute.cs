namespace JobMaster.Abstractions.Models.Attributes;

/// <summary>
/// Routes all jobs and recurring schedules for this handler to a specific cluster by default.
/// Can be overridden per-call via the <c>clusterId</c> parameter on scheduler methods, and for static
/// recurring schedules by the profile's <c>ClusterId</c>.
/// If omitted, the default cluster is used.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class JobMasterClusterIdAttribute : Attribute
{
    /// <summary>Initializes the attribute with the specified cluster id.</summary>
    public JobMasterClusterIdAttribute(string clusterId)
    {
        this.ClusterId = clusterId;
    }

    /// <summary>The cluster that jobs scheduled for this handler will be routed to by default.</summary>
    public string ClusterId { get; }
}
