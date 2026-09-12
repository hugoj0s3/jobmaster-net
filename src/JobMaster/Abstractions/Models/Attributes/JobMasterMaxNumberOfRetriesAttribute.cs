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
    /// <summary>Initializes the attribute with the specified retry limit.</summary>
    /// <param name="maxNumberOfRetries">The maximum number of retries. Must not exceed <see cref="JobMasterConstants.MaxAllowedRetries"/>.</param>
    public JobMasterMaxNumberOfRetriesAttribute(int maxNumberOfRetries)
    {
        if (maxNumberOfRetries > JobMasterConstants.MaxAllowedRetries)
        {
            throw new ArgumentException($"MaxNumberOfRetries must be less than or equal to {JobMasterConstants.MaxAllowedRetries}.");
        }
        MaxNumberOfRetries = maxNumberOfRetries;
    }

    /// <summary>The maximum number of automatic retries configured for this handler.</summary>
    public int MaxNumberOfRetries { get; }
}
