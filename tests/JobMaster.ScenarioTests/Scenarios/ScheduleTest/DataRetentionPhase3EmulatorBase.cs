using FluentAssertions;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest;

/// <summary>
/// Shared phase-3 logic: once Phase2 has proven the real drain lifecycle destroyed every bucket on
/// <see cref="DrainClusterId"/>, this phase retires the connection entirely. By the time
/// <see cref="RunAsync"/> runs, the base framework has already restarted
/// <see cref="DrainClusterId"/>'s container with only its Coordinator worker (the Drain workers are
/// gone) -- since a Drain-mode worker keeps heartbeating its AgentConnectionId indefinitely
/// (LoadAgentConnectionRunnersAsync runs for every non-Coordinator mode) even once it has nothing
/// left to drain, removing them entirely is what actually lets the connection go idle.
///
/// Asserts the connections go dead fast (<c>ApiAgentConnection.IsAlive</c>, ~45s via
/// ResourceAliveThreshold) and then, much later, get physically deleted by
/// CleanupDeadAgentConnectionsRunner's own hardcoded, non-configurable 30-minute dead threshold
/// plus its own 5-minute tick -- only possible because Phase2 already proved this cluster has no
/// buckets left (HasBucketsAsync gates SafeDeleteConnectionAsync).
/// </summary>
public abstract class DataRetentionPhase3EmulatorBase<TPhaseEnum>(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<TPhaseEnum>(global, runner)
    where TPhaseEnum : struct, Enum
{
    private static readonly TimeSpan AliveFlipWaitTimeout = TimeSpan.FromMinutes(3);

    // CleanupDeadAgentConnectionsRunner's dead-connection threshold before it actually deletes a
    // connection row is a hardcoded, non-configurable 30 minutes (DeleteAgentConnectionThreshold),
    // plus its own 5-minute tick interval on top.
    private static readonly TimeSpan ConnectionDeletionWaitTimeout = TimeSpan.FromMinutes(40);

    protected abstract string DrainClusterId { get; }

    public override async Task RunAsync()
    {
        var api = Runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");

        // Every bucket must already be gone -- Phase2 proved this before this phase's container even
        // started. Confirms the restart didn't somehow bring any back.
        (await api.GetBucketCountAsync(DrainClusterId)).Should().Be(0);

        // The Drain workers are gone (only the Coordinator remains), so nothing heartbeats these
        // connections anymore -- they should flip dead quickly. Excludes the Coordinator's own
        // reserved fallback connection, which is never heartbeated by design (already dead).
        await PollingWaitUtil.WaitUntilAsync(AliveFlipWaitTimeout,
            async () => (await api.GetAgentConnectionsAsync(DrainClusterId)).ExcludingReserved().All(c => !c.IsAlive),
            $"agent connections on {DrainClusterId} to go dead once their Drain workers are retired");

        // Dead is not the same as deleted: CleanupDeadAgentConnectionsRunner only physically removes
        // a connection once it's been dead for its own hardcoded 30-minute threshold -- and only
        // because Phase2 already proved it has no buckets left. Excludes the reserved fallback
        // connection, which CleanupDeadAgentConnectionsRunner explicitly never deletes by design --
        // waiting for it to reach zero would never succeed.
        await PollingWaitUtil.WaitUntilAsync(ConnectionDeletionWaitTimeout,
            async () => !(await api.GetAgentConnectionsAsync(DrainClusterId)).ExcludingReserved().Any(),
            $"agent connections on {DrainClusterId} to be physically deleted after going unused");
    }
}
