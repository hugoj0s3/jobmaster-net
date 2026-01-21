namespace JobMaster.Abstractions.Models.Attributes;

[AttributeUsage(AttributeTargets.Class)]
public class JobMasterTimeoutAttribute : Attribute
{
    public JobMasterTimeoutAttribute(double timeoutInSeconds)
    {
        this.TimeoutInSeconds = timeoutInSeconds;
    }

    public double TimeoutInSeconds { get; }
}