namespace JobMaster.Abstractions.Models;

/// <summary>
/// Defines the operational mode of a JobMaster cluster.
/// </summary>
public enum ClusterMode
{
    /// <summary>Normal operation — the cluster schedules and dispatches jobs.</summary>
    Active = 1,

    /// <summary>
    /// The cluster is registered but does not dispatch jobs locally — jobs and recurring schedules
    /// are held on master and continuously forwarded to a mandatory <c>TargetActiveClusterId</c>
    /// cluster instead. Used for online migration: point traffic at a new Active cluster, flip this
    /// one into Migrating mode, and let it drain out (pre-existing buckets finish via Drain workers;
    /// anything not yet dispatched is forwarded) without stopping traffic or losing work.
    /// </summary>
    Migrating = 2,

    /// <summary>The cluster is retained for historical data only.</summary>
    Archived = 3,
}
