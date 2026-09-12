namespace JobMaster.Abstractions.StaticRecurringSchedules;

#if NET7_0_OR_GREATER
/// <summary>
/// Defines a profile of static recurring schedules that are registered once at application startup.
/// Implement this interface to declare recurring jobs whose recurrence expressions are fixed at deploy time.
/// JobMaster discovers all implementations automatically when the cluster is configured.
/// </summary>
public interface IStaticRecurringSchedulesProfile
{
    /// <summary>
    /// A unique identifier for this profile across the entire cluster.
    /// Must follow the JobMaster identifier format (letters, numbers, hyphens, underscores, dots).
    /// </summary>
    static abstract string ProfileId { get; }

    /// <summary>
    /// The cluster this profile is scoped to. An empty string targets the default (single) cluster.
    /// Override when running multiple clusters in the same process.
    /// </summary>
    static virtual string ClusterId => string.Empty;

    /// <summary>
    /// The worker lane all schedules in this profile will be assigned to.
    /// <c>null</c> means any worker regardless of lane can process the generated jobs.
    /// </summary>
    static virtual string? WorkerLane => null;

    /// <summary>
    /// Registers the recurring schedule definitions for this profile by populating <paramref name="collection"/>.
    /// Called once at startup; the definitions are then persisted and kept alive by the cluster.
    /// </summary>
    static abstract void Config(RecurringScheduleDefinitionCollection collection);
}
#else
/// <summary>
/// Defines a profile of static recurring schedules that are registered once at application startup.
/// Implement this interface to declare recurring jobs whose recurrence expressions are fixed at deploy time.
/// JobMaster discovers all implementations automatically when the cluster is configured.
/// </summary>
public interface IStaticRecurringSchedulesProfile
{
}
#endif
