namespace JobMaster.Abstractions.Models.Attributes;

/// <summary>
/// Sets the default execution timeout (in seconds) for all jobs scheduled for this handler.
/// Can be overridden per-call via the <c>timeout</c> parameter on scheduler methods.
/// If omitted, the cluster-level default applies.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class JobMasterTimeoutAttribute : Attribute
{
    /// <summary>Initializes the attribute with the specified timeout in seconds.</summary>
    public JobMasterTimeoutAttribute(double timeoutInSeconds)
    {
        this.TimeoutInSeconds = timeoutInSeconds;
    }

    /// <summary>The default execution timeout in seconds for jobs scheduled for this handler.</summary>
    public double TimeoutInSeconds { get; }
}
