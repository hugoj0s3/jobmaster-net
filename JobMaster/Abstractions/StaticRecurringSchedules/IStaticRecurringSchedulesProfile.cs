namespace JobMaster.Abstractions.StaticRecurringSchedules;

#if NET7_0_OR_GREATER
public interface IStaticRecurringSchedulesProfile
{
    static abstract string ProfileId { get; }

    static virtual string ClusterId => string.Empty;
    
    static virtual string? WorkerLane => null;
    
    static abstract void Config(RecurringScheduleDefinitionCollection collection);
}
#else
// Fallback marker interface for older target frameworks (no static interface members support)
public interface IStaticRecurringSchedulesProfile
{
}
#endif