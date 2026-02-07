using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.RecurrenceExpressions.TimeSpanInterval;

namespace JobMaster.SampleWeb;

public class HelloRecurringScheduleProfile : IStaticRecurringSchedulesProfile
{
    public static string ProfileId => "HelloRecurringScheduleProfile";
    
    public static void Config(RecurringScheduleDefinitionCollection collection)
    {
        collection.Add<HelloJobHandler>(TimeSpanIntervalExprCompiler.TypeId, "00:00:25");
    }
}