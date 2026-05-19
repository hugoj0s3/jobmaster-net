using JobMaster.Sdk.Abstractions;

namespace JobMaster.Abstractions.Models.Attributes;

/// <summary>
/// Sets the default maximum number of automatic retries for all jobs scheduled for this handler.
/// Can be overridden per-call via the <c>maxNumberOfRetries</c> parameter on scheduler methods.
/// If omitted, the value configured on the cluster is used.
/// </summary>
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
