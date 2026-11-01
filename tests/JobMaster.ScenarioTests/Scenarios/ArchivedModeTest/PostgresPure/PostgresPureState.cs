namespace JobMaster.ScenarioTests.Scenarios.ArchivedModeTest.PostgresPure;

/// <summary>
/// Phase2 -> Phase3 hand-off (same static-holder approach as StandaloneToDistributedTest's state): the
/// late jobs and schedule Phase2 left on stray-old, which Phase3 asserts end up on stray-new. Safe as
/// static state because scenarios run in one serialized collection (<c>ScenarioCollection</c>).
/// </summary>
internal static class PostgresPureState
{
    public static string TestIdentifier { get; set; } = "";
    public static string RecurringTestIdentifier { get; set; } = "";
    public static List<Guid> LateJobIds { get; set; } = new();
    public static Guid LateRecurringScheduleId { get; set; }
}
