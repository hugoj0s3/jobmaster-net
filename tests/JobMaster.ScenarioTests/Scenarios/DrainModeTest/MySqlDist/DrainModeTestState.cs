namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.MySqlDist;

/// <summary>
/// Phase1/Phase2/Phase3 emulators are separate <c>Activator.CreateInstance</c> instances with no
/// direct reference to each other (see <c>BaseScenarioEmulator.CreatePhaseEmulators</c>) -- this is
/// the one deliberate exception, a small static holder for the specific set of bucket IDs Phase1's
/// executors owned before they were crashed. Populated by Phase2 (captured right at its start,
/// before anything else could create a new bucket on the same connections), read by Phase3 to
/// assert precisely those buckets -- and no others -- were fully destroyed by the drain, even once
/// Phase3's own returning executors have created their own fresh buckets on the very same
/// connections. Safe as static state here specifically because DrainModeTest.MySqlDist runs
/// within JobMaster.ScenarioTests' single serialized collection (<c>ScenarioCollection</c>) -- never
/// concurrently with another scenario run in the same process.
/// </summary>
internal static class DrainModeTestState
{
    public static List<string> OriginalBucketIds { get; set; } = new();
}
