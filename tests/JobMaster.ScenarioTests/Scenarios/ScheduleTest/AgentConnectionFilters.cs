using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest;

internal static class AgentConnectionFilters
{
    // Mirrors JobMasterConstants.ReservedAgentConnectionNamePrefix -- framework-internal connections
    // (e.g. a Coordinator worker's own fallback-bucket connection, auto-created regardless of what
    // this scenario configured) are never heartbeated the normal way and are explicitly excluded
    // from CleanupDeadAgentConnectionsRunner's deletion (they're meant to persist indefinitely), so
    // they must be excluded from both "should be alive" and "should eventually be deleted"
    // assertions -- otherwise a "wait for connection count == 0" check would never succeed.
    private const string ReservedNamePrefix = "JMReserved-";

    public static IEnumerable<ApiAgentConnection> ExcludingReserved(this IEnumerable<ApiAgentConnection> connections) =>
        connections.Where(c => !c.Name.StartsWith(ReservedNamePrefix, StringComparison.OrdinalIgnoreCase));
}
