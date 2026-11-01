namespace JobMaster.Abstractions.Models;

/// <summary>
/// Defines the operational mode of a JobMaster cluster.
/// </summary>
public enum ClusterMode
{
    /// <summary>Normal operation — the cluster schedules and dispatches jobs.</summary>
    Active = 1,

    /// <summary>
    /// The cluster never locally dispatches or executes new work — <c>OnMaster</c> jobs and recurring
    /// schedules are held on master and continuously moved (bulk-inserted, then deleted from here) to
    /// a mandatory <c>TargetActiveClusterId</c> cluster, which does the actual acquire/dispatch/execute
    /// cycle instead. Used for online migration: point traffic at a new Active cluster, flip this one
    /// into Migrating mode, and let it drain out (buckets already dispatched before the flip still
    /// finish locally via Drain workers; everything still <c>OnMaster</c> is moved) without stopping
    /// traffic or losing work.
    /// </summary>
    Migrating = 2,

    /// <summary>
    /// The cluster is retained for historical data only and no longer accepts new jobs or recurring schedules:
    /// scheduling or rescheduling on it throws an <see cref="System.InvalidOperationException"/>.
    /// <para>
    /// With a target (<c>Archive(targetActiveClusterId)</c>, or kept from <c>MigrateTo</c>), unfinished jobs and
    /// active recurring schedules on the cluster, including ones that reach it later, are moved to the target and
    /// run there.
    /// </para>
    /// <para>
    /// Requires one worker of the archived cluster, running in an app that also configures the target.
    /// Until one runs, the jobs wait on the archived cluster.
    /// </para>
    /// <para>Without a target, startup fails while the cluster still holds unfinished jobs or schedules.</para>
    /// </summary>
    Archived = 3,
}
