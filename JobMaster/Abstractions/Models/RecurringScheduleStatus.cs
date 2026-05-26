namespace JobMaster.Abstractions.Models;

/// <summary>
/// Represents the lifecycle status of a recurring schedule.
/// </summary>
public enum RecurringScheduleStatus
{
    /// <summary>The schedule has been created locally but not yet persisted to the master database.</summary>
    PendingSave = 1,
    /// <summary>The schedule is running and generating job occurrences.</summary>
    Active = 2,
    /// <summary>The schedule was explicitly stopped before reaching its end date.</summary>
    Canceled = 3,
    /// <summary>The schedule is paused or has no remaining occurrences within its configured window.</summary>
    Inactive = 4,
    /// <summary>The schedule reached its natural end and will not generate further jobs.</summary>
    Completed = 5,
}

/// <summary>Extension methods for <see cref="RecurringScheduleStatus"/>.</summary>
public static class RecurringScheduleStatusExtensions
{
    /// <summary>
    /// Returns <c>true</c> if the schedule is in a non-active but not yet finalized state
    /// (<see cref="RecurringScheduleStatus.Canceled"/> or <see cref="RecurringScheduleStatus.Inactive"/>).
    /// </summary>
    public static bool IsCanceledOrInactive(this RecurringScheduleStatus status)
    {
        return status == RecurringScheduleStatus.Canceled ||
               status == RecurringScheduleStatus.Inactive;

    }

    /// <summary>
    /// Returns <c>true</c> if the schedule has reached a terminal state and will not generate further jobs
    /// (<see cref="RecurringScheduleStatus.Canceled"/>, <see cref="RecurringScheduleStatus.Completed"/>,
    /// or <see cref="RecurringScheduleStatus.Inactive"/>).
    /// </summary>
    public static bool IsFinalStatus(this RecurringScheduleStatus status)
    {
        return status == RecurringScheduleStatus.Canceled ||
               status == RecurringScheduleStatus.Completed ||
               status == RecurringScheduleStatus.Inactive;
    }
}
