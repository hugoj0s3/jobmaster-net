namespace JobMaster.Contracts.Models.Attributes;

[AttributeUsage(AttributeTargets.Class)]
public class JobMasterWorkerLaneAttribute : Attribute
{
    public JobMasterWorkerLaneAttribute(string workerLane)
    {
        this.WorkerLane = workerLane;
    }

    public string WorkerLane { get; }
}