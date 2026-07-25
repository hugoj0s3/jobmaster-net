namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest.MySqlPure;

/// <summary>
/// Phase1/Phase2 emulators are separate <c>Activator.CreateInstance</c> instances with no direct
/// reference to each other (see <c>BaseScenarioEmulator.CreatePhaseEmulators</c>) -- this is the
/// one deliberate exception, a small static holder for the test identifier and exact set of job IDs
/// Phase1 scheduled against the standalone worker before it was crashed. Populated by Phase1, read
/// by Phase2 to assert precisely those jobs -- and no others -- reached Succeeded on the new
/// distributed cluster. Safe as static state here specifically because
/// StandaloneToDistributedTest.MySqlPure runs within JobMaster.ScenarioTests' single serialized
/// collection (<c>ScenarioCollection</c>) -- never concurrently with another scenario run in the
/// same process.
/// </summary>
internal static class MySqlPureState
{
    public static string TestIdentifier { get; set; } = "";
    public static List<Guid> ScheduledJobIds { get; set; } = new();
}
