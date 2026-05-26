namespace JobMaster.Abstractions.Models.Attributes;

/// <summary>
/// Routes all jobs scheduled for this handler to a specific worker lane by default.
/// Can be overridden per-call via the <c>workerLane</c> parameter on scheduler methods.
/// If omitted, the jobs run on the default worker lane (null).
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class JobMasterWorkerLaneAttribute : Attribute
{
    /// <summary>Initializes the attribute with the specified worker lane name.</summary>
    public JobMasterWorkerLaneAttribute(string workerLane)
    {
        this.WorkerLane = workerLane;
    }

    /// <summary>The worker lane that jobs scheduled for this handler will be routed to by default.</summary>
    public string WorkerLane { get; }
}
