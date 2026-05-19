namespace JobMaster.Abstractions.Models.Attributes;

/// <summary>
/// Routes all jobs scheduled for this handler to a specific worker lane by default.
/// Can be overridden per-call via the <c>workerLane</c> parameter on scheduler methods.
/// If omitted, the jobs run on the default worker lane (null).
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class JobMasterWorkerLaneAttribute : Attribute
{
    public JobMasterWorkerLaneAttribute(string workerLane)
    {
        this.WorkerLane = workerLane;
    }

    public string WorkerLane { get; }
}
