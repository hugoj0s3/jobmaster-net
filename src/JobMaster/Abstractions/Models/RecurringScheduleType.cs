namespace JobMaster.Abstractions.Models;

/// <summary>
/// Distinguishes how a recurring schedule's recurrence expression is managed.
/// </summary>
public enum RecurringScheduleType
{
    /// <summary>The schedule is defined at application startup via <see cref="JobMaster.Abstractions.StaticRecurringSchedules.IStaticRecurringSchedulesProfile"/>.</summary>
    Static = 2,
    /// <summary>The schedule is created and managed at runtime through the scheduler API.</summary>
    Dynamic = 3,
}
