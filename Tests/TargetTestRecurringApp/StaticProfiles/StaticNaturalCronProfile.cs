using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.RecurrenceExpressions.NaturalCron;
using NaturalCron;
using NaturalCron.Builder;
using TargetTestRecurringApp.Handlers;

namespace TargetTestRecurringApp.StaticProfiles;

public class StaticNaturalCronProfile : IStaticRecurringSchedulesProfile
{
    // Duplicated as a literal on the scenario-test side (different process/project) -- same
    // pairing pattern already used for JobDefinitionId strings between TargetTestScheduleApp and
    // PureScheduleTestPhase1EmulatorBase.
    public const string TestIdentifier = "static-natural-cron";

    public static string ProfileId => "StaticNaturalCronProfile";

    public static void Config(RecurringScheduleDefinitionCollection collection)
    {
        NaturalCronExpr expr = NaturalCronBuilder.Every(6).Minutes().Build();
        collection.Add<RecurringTickHandler>(
            expr,
            metadata: WritableMetadata.New().SetStringValue("TestIdentifier", TestIdentifier));
    }
}
