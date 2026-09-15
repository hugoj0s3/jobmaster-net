using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.NCrontab;
using TargetTestRecurringApp.Handlers;

namespace TargetTestRecurringApp.StaticProfiles;

public class StaticNCrontabProfile : IStaticRecurringSchedulesProfile
{
    // Duplicated as a literal on the scenario-test side (different process/project) -- same
    // pairing pattern already used for JobDefinitionId strings between TargetTestScheduleApp and
    // PureScheduleTestPhase1EmulatorBase.
    public const string TestIdentifier = "static-ncrontab";

    public static string ProfileId => "StaticNCrontabProfile";

    public static void Config(RecurringScheduleDefinitionCollection collection)
    {
        collection.Add<RecurringTickHandler>(
            "*/6 * * * *",
            metadata: WritableMetadata.New().SetStringValue("TestIdentifier", TestIdentifier));
    }
}
