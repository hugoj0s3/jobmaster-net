using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.RecurrenceExpressions.TimeSpanInterval;
using TargetTestClusterIdApp.Handlers;

namespace TargetTestClusterIdApp.StaticProfiles;

public class TargetClusterProfile : IStaticRecurringSchedulesProfile
{
    // Duplicated as a literal on the scenario-test side (different process/project).
    public const string TestIdentifier = "cluster-id-profile";

    public static string ProfileId => "TargetClusterProfile";

    public static string ClusterId => ClusterIds.Target;

    public static void Config(RecurringScheduleDefinitionCollection collection)
    {
        collection.Add<ProfileTickHandler>(
            TimeSpanIntervalExprCompiler.TypeId,
            "00:00:15",
            metadata: WritableMetadata.New().SetStringValue("TestIdentifier", TestIdentifier));
    }
}
