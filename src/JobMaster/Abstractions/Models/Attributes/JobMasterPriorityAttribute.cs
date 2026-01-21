namespace JobMaster.Abstractions.Models.Attributes;

[AttributeUsage(AttributeTargets.Class)]
public class JobMasterPriorityAttribute : Attribute
{
    public JobMasterPriority Priority { get; }
    
    public JobMasterPriorityAttribute(JobMasterPriority priority)
    {
        Priority = priority;
    }
}