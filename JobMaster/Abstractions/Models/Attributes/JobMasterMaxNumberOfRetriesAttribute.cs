using JobMaster.Sdk.Abstractions;

namespace JobMaster.Abstractions.Models.Attributes;

[AttributeUsage(AttributeTargets.Class)]
public class JobMasterMaxNumberOfRetriesAttribute : Attribute
{
    public JobMasterMaxNumberOfRetriesAttribute(int maxNumberOfRetries)
    {
        if (maxNumberOfRetries > JobMasterConstants.MaxAllowedRetries)
        {
            throw new ArgumentException($"MaxNumberOfRetries must be less than or equal to {JobMasterConstants.MaxAllowedRetries}.");
        }
        MaxNumberOfRetries = maxNumberOfRetries;
    }

    public int MaxNumberOfRetries { get; }
}