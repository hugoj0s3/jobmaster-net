namespace JobMaster.Abstractions.Models;

public enum ClusterMode
{
    /// <summary>Normal operation — the cluster schedules and dispatches jobs.</summary>
    Active = 1,

    /// <summary>The cluster is registered but does not dispatch jobs.</summary>
    Passive = 2,

    /// <summary>The cluster is retained for historical data only.</summary>
    Archived = 3,
}
