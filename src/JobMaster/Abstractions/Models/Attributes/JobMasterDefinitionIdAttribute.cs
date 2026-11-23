namespace JobMaster.Abstractions.Models.Attributes;

/// <summary>
/// Assigns a stable, human-readable ID to a job handler class.
/// The runtime uses this ID to locate the correct handler when a job is dispatched.
/// If omitted, the handler's full type name (<see cref="Type.FullName"/>) is used as the ID.
/// Prefer setting an explicit ID to avoid breakage if the class is renamed or moved to different namespace.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class JobMasterDefinitionIdAttribute : Attribute
{
    /// <summary>Initializes the attribute with the given definition ID.</summary>
    /// <param name="jobDefinitionId">The stable, human-readable ID to assign to the handler.</param>
    public JobMasterDefinitionIdAttribute(string jobDefinitionId)
    {
        JobDefinitionId = jobDefinitionId;
    }

    /// <summary>The stable, human-readable ID assigned to the job handler.</summary>
    public string JobDefinitionId { get; }
}
