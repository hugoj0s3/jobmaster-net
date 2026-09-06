using FluentAssertions;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;
using JobMaster.ScenarioTests.Scenarios.ScheduleTest;

namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.PostgresNats;

/// <summary>
/// Drain-only recovery: all 3 executors died at the end of Phase1 immediately after scheduling, with
/// jobs caught across every pipeline stage -- some still PendingSave, some OnMaster, some already
/// InBucket/Onboarded/Queued/Processing. This phase's topology is a Coordinator plus 3 Drain
/// workers, each on the *same* agent connection its dead counterpart used (drainer-1/nats-agent-1,
/// drainer-2/nats-agent-2, drainer-3/nats-agent-3) -- AssignedLostBucketsRunner reassigns a dead
/// worker's orphaned buckets to any live worker on the *same* connection whose mode is Drain or
/// Full, so each drainer inherits its own connection's leftover buckets directly, and recovers them
/// through both PollingDrainSavePendingJobsRunner (PendingSave jobs) and
/// PollingDrainProcessingJobsRunner (already-onboarded jobs) -- here, NATS JetStream's own
/// equivalents (NatsJetStreamDrainSavePendingJobsRunner / NatsJetStreamDrainProcessingRunner).
///
/// This phase deliberately checks only one thing, fast: every job made it durably into the master
/// store, regardless of what status it ended up in. It does *not* wait for the bucket lifecycle to
/// finish destroying the old buckets -- that's a slow (tens of minutes), separate administrative
/// concern with no bearing on job safety, checked instead in Phase3 alongside job execution. It does
/// capture the exact set of bucket IDs Phase1's dead executors owned (see
/// <see cref="DrainModeTestState"/>), so Phase3 can assert precisely those buckets get destroyed.
/// See scenario.md for the full reasoning and the empirical timing breakdown that motivated
/// splitting it out this way (inherited from DrainModeTest.PostgresDist, this scenario's template).
/// </summary>
public sealed class PostgresNatsPhase2Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<PostgresNatsPhases>(global, runner)
{
    private const string ClusterId = "postgres-nats-drain-load";
    private static readonly string[] AgentConnectionNames = ["nats-agent-1", "nats-agent-2", "nats-agent-3"];

    // Every job sits in PendingSave (agent-side transport only, not yet visible via this
    // master-backed count) for a short window after being scheduled, and -- for jobs that were
    // mid-drain when the crash hit -- until PollingDrainSavePendingJobsRunner recovers them. This is
    // the only thing this phase waits on, so it should resolve quickly.
    private static readonly TimeSpan AllJobsSavedTimeout = TimeSpan.FromMinutes(2);

    public override PostgresNatsPhases Phase() => PostgresNatsPhases.Phase2;

    public override async Task RunAsync()
    {
        var api = Runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");

        var connections = (await api.GetAgentConnectionsAsync(ClusterId)).ExcludingReserved().ToList();
        connections.Should().HaveCount(3);
        connections.Should().OnlyContain(c => c.IsAlive);
        connections.Select(c => c.Name).Should().BeEquivalentTo(AgentConnectionNames);

        // Capture exactly which buckets Phase1's (now dead) executors owned, right now -- before
        // anything else in this scenario could ever create a new one on these connections. Phase3
        // uses this to assert precisely these buckets get destroyed, distinct from whatever fresh
        // buckets its own returning executors create later on the same connections.
        var originalBuckets = new List<string>();
        foreach (var name in AgentConnectionNames)
        {
            var buckets = await api.GetBucketsAsync(ClusterId, agentConnectionId: connections.Single(c => c.Name == name).Id);
            originalBuckets.AddRange(buckets.Select(b => b.Id));
        }

        originalBuckets.Should().NotBeEmpty("Phase1's executors must have created buckets before crashing");
        DrainModeTestState.OriginalBucketIds = originalBuckets;

        // The only thing that matters here: no job was lost by the crash -- every one of them made
        // it durably into the master store. Doesn't matter which status it's in.
        await PollingWaitUtil.WaitUntilAsync(AllJobsSavedTimeout,
            async () => await api.GetJobCountAsync(ClusterId) == DrainModeTestPlan.TotalJobs,
            "every job to be durably saved in the master store",
            pollInterval: TimeSpan.FromSeconds(5));
    }
}
