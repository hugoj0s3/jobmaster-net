namespace JobMaster.Contracts.Models.Attributes;

[AttributeUsage(AttributeTargets.Class)]
public class JobMasterMaxNumberOfRetriesAttribute : Attribute
{
    public JobMasterMaxNumberOfRetriesAttribute(int maxNumberOfRetries)
    {
        if (maxNumberOfRetries > 10)
        {
            throw new ArgumentException("MaxNumberOfRetries must be less than or equal to 10.");
        }
        MaxNumberOfRetries = maxNumberOfRetries;
    }

    public int MaxNumberOfRetries { get; }
}