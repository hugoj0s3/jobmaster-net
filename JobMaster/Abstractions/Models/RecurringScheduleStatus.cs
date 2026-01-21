namespace JobMaster.Abstractions.Models;

public enum RecurringScheduleStatus
{
    PendingSave = 1,
    Active = 2,
    Canceled = 3,
    Inactive = 4,
    Completed = 5,
}

public static class RecurringScheduleStatusExtensions
{
    public static bool IsCanceledOrInactive(this RecurringScheduleStatus status)
    {
        return status == RecurringScheduleStatus.Canceled ||
               status == RecurringScheduleStatus.Inactive;

    }
    
    public static bool IsFinalStatus(this RecurringScheduleStatus status)
    {
        return status == RecurringScheduleStatus.Canceled || 
               status == RecurringScheduleStatus.Completed || 
               status == RecurringScheduleStatus.Inactive;
    }
}