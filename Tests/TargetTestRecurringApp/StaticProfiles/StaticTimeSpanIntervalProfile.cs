using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.RecurrenceExpressions.TimeSpanInterval;
using TargetTestRecurringApp.Handlers;

namespace TargetTestRecurringApp.StaticProfiles;

public class StaticTimeSpanIntervalProfile : IStaticRecurringSchedulesProfile
{
    // Duplicated as a literal on the scenario-test side (different process/project) -- same
    // pairing pattern already used for JobDefinitionId strings between TargetTestScheduleApp and
    // PureScheduleTestPhase1EmulatorBase.
    public const string TestIdentifier = "static-timespan-interval";

    public static string ProfileId => "StaticTimeSpanIntervalProfile";

    public static void Config(RecurringScheduleDefinitionCollection collection)
    {
        collection.Add<RecurringTickHandler>(
            TimeSpanIntervalExprCompiler.TypeId,
            "00:06:00",
            metadata: WritableMetadata.New().SetStringValue("TestIdentifier", TestIdentifier));
    }
}
