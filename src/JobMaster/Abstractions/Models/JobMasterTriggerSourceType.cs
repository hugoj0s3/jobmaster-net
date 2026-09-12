namespace JobMaster.Abstractions.Models;

/// <summary>
/// Defines how a job is triggered.
/// </summary>
public enum JobMasterTriggerSourceType
{
    /// <summary>The job runs a single time at its scheduled date and time.</summary>
    Once = 1,
    /// <summary>The job is driven by a static recurring schedule defined at application startup.</summary>
    StaticRecurring = 2,
    /// <summary>The job is driven by a dynamic recurring schedule created at runtime.</summary>
    DynamicRecurring = 3,
}

/// <summary>Extension methods for <see cref="JobMasterTriggerSourceType"/>.</summary>
public static class JobMasterTriggerSourceTypeExtension
{
    /// <summary>
    /// Returns <c>true</c> if the source type is any recurring variant (Static or Dynamic).
    /// </summary>
    public static bool IsRecurringTrigger(this JobMasterTriggerSourceType sourceType)
    {
        return sourceType == JobMasterTriggerSourceType.StaticRecurring || sourceType == JobMasterTriggerSourceType.DynamicRecurring;
    }
}
