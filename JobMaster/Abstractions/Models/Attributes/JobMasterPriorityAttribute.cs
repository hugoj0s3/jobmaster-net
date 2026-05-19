namespace JobMaster.Abstractions.Models.Attributes;

/// <summary>
/// Sets the default <see cref="JobMasterPriority"/> for all jobs scheduled for this handler.
/// Can be overridden per-call via the <c>priority</c> parameter on scheduler methods.
/// If omitted, <see cref="JobMasterPriority.Medium"/> is used.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class JobMasterPriorityAttribute : Attribute
{
    public JobMasterPriority Priority { get; }

    public JobMasterPriorityAttribute(JobMasterPriority priority)
    {
        Priority = priority;
    }
}
