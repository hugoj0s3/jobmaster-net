

using JobMaster.Internals;

namespace JobMaster.Abstractions.StaticRecurringSchedules;

public class StaticRecurringSchedulesProfileInfo
{
    public StaticRecurringSchedulesProfileInfo(string profileId, string clusterId, string? workerLane)
    {
        ProfileId = profileId;
        ClusterId = clusterId;
        WorkerLane = workerLane;
    }
    
    public string ProfileId { get; private set; }
    public string ClusterId { get; private set; }
    public string? WorkerLane { get; private set; }

    public bool IsValid => JobMasterStringUtils.IsValidForId(ProfileId) && JobMasterStringUtils.IsValidForId(ClusterId);
}