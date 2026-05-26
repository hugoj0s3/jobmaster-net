

using JobMaster.Sdk.Utils;

namespace JobMaster.Abstractions.StaticRecurringSchedules;

/// <summary>
/// Holds resolved metadata for a registered <see cref="IStaticRecurringSchedulesProfile"/>.
/// Used internally to track and validate profiles that have been discovered at startup.
/// </summary>
public class StaticRecurringSchedulesProfileInfo
{
    /// <summary>Initializes a new profile info record.</summary>
    public StaticRecurringSchedulesProfileInfo(string profileId, string clusterId, string? workerLane)
    {
        ProfileId = profileId;
        ClusterId = clusterId;
        WorkerLane = workerLane;
    }

    /// <summary>The unique profile identifier.</summary>
    public string ProfileId { get; private set; }

    /// <summary>The cluster this profile is scoped to.</summary>
    public string ClusterId { get; private set; }

    /// <summary>The worker lane, or <c>null</c> if the profile targets any lane.</summary>
    public string? WorkerLane { get; private set; }

    /// <summary>
    /// Returns <c>true</c> if both <see cref="ProfileId"/> and <see cref="ClusterId"/> pass
    /// the JobMaster identifier format rules.
    /// </summary>
    public bool IsValid => JobMasterStringUtils.IsValidForId(ProfileId) && JobMasterStringUtils.IsValidForId(ClusterId);
}
