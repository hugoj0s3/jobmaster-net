using FluentAssertions;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;
using JobMaster.ScenarioTests.Scenarios.ScheduleTest;

namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.RavenDbDist;

/// <summary>
/// Finalization: real Execution capacity returns -- executor-1/2/3, each back on its own original
/// connection (raven-agent-1/2/3), pooled alongside drainer-1/2/3 which keep running in the background
/// (Phase2's containers aren't stopped just because this phase doesn't re-list them). Whatever
/// Phase2 left parked as OnMaster gets onboarded and executed to completion, proving the whole
/// crash-drain-recover cycle actually finishes correctly under load. Also asserts the bucket
/// lifecycle, lightly: the exact set of buckets Phase2 captured (<see
/// cref="DrainModeTestState.OriginalBucketIds"/> -- Phase1's dead executors' own buckets) must each
/// reach ReadyToDelete or already be gone, even while the new executors are creating and actively
/// using their own fresh buckets on the very same connections. Checking by specific captured ID
/// rather than connection-wide count or status is what makes this precise: a connection-wide count
/// would never reach 0 again once the new executors' own buckets exist, and a generic transient-status
/// check couldn't tell "an old bucket" from "a new one mid-transition". Deliberately stops at
/// ReadyToDelete rather than waiting for physical destruction: the two legs are comparable in
/// duration (both gated by the same JobMasterConstants.BucketNoJobsBeforeReadyToDelete constant --
/// ~10-15 min for Draining-to-ReadyToDelete, another ~10-20 min for ReadyToDelete-to-destroyed), and
/// the second leg is already covered in isolation by DestroyReadyToDeleteBucketsRunnerTests -- this
/// scenario test's job is to prove no job was lost or duplicated, not to re-prove destruction timing
/// under load.
/// </summary>
public sealed class RavenDbDistPhase3Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<RavenDbDistPhases>(global, runner)
{
    private const string ClusterId = "ravendb-drain-load";
    private const int SucceededStatus = 5;
    private static readonly string[] AgentConnectionNames = ["raven-agent-1", "raven-agent-2", "raven-agent-3"];

    // Onboarding the OnMaster backlog plus executing whatever's left through 3 live workers, at
    // 5000-job volume. Widened from 15 to 45 minutes: Phase2 no longer waits for the old buckets to
    // fully drain before this phase starts, so the new executors now onboard/execute concurrently
    // with the still-very-active old drainers -- real contention on the Coordinator's shared runner
    // semaphore (AssignJobsToBucketsRunner competing with MarkBucketAsLostRunner/
    // AssignedLostBucketsRunner/DestroyReadyToDeleteBucketsRunner for the same MainSemaphoreSlim
    // turns), not a regression. One run only got 3072/3500 "fast" jobs done in 15 minutes.
    private static readonly TimeSpan FinalizeTimeout = TimeSpan.FromMinutes(45);

    // Real drain-to-ReadyToDelete timing for the old buckets, running in the background since Phase2
    // -- worker-death detection, Lost -> ReadyToDrain reassignment, Draining -> the 10-minute
    // BucketNoJobsBeforeReadyToDelete wait, real unmodified JobMaster timing. Only waits for
    // ReadyToDelete (or already destroyed), not physical destruction -- that second leg is a
    // comparable-duration wait already covered by DestroyReadyToDeleteBucketsRunnerTests, and most of
    // this window elapses "for free" during the FinalizeTimeout wait above since both run in parallel.
    private static readonly TimeSpan OldBucketsResolvedTimeout = TimeSpan.FromMinutes(20);
    private const int ReadyToDeleteStatus = 6;

    public override RavenDbDistPhases Phase() => RavenDbDistPhases.Phase3;

    public override async Task RunAsync()
    {
        var api = Runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");

        var connections = (await api.GetAgentConnectionsAsync(ClusterId)).ExcludingReserved().ToList();
        connections.Should().HaveCount(3);
        connections.Should().OnlyContain(c => AgentConnectionNames.Contains(c.Name) && c.IsAlive);

        // The core guarantee: every job saved, executed exactly once, none lost, none duplicated.
        foreach (var batch in DrainModeTestPlan.Batches)
        {
            var executions = await Runner.Tracker.WaitForAsync(batch.TestIdentifier, batch.QtyJobs, FinalizeTimeout);
            executions.Select(e => e.JobId).Should().OnlyHaveUniqueItems();

            var jobDefinitionId = DrainModeTestPlan.HandlerTypeToJobDefinitionId[batch.HandlerType];
            var apiJobs = await api.GetJobsAsync(ClusterId, jobDefinitionId, batch.Priority, batch.TestIdentifier, status: SucceededStatus, countLimit: int.MaxValue);

            apiJobs.Should().HaveCount(batch.QtyJobs);
            apiJobs.Select(j => GuidBase64.Parse(j.Id)).Should().BeEquivalentTo(executions.Select(e => e.JobId),
                "the Redis-tracked executions and the API-persisted Succeeded jobs must be exactly the same set -- no job lost, none executed twice");
        }

        (await api.GetJobCountAsync(ClusterId, status: SucceededStatus)).Should().Be(DrainModeTestPlan.TotalJobs);
        (await api.GetJobCountAsync(ClusterId)).Should().Be(DrainModeTestPlan.TotalJobs);

        // The bucket lifecycle, lightly: exactly the buckets Phase1's dead executors owned must each
        // reach ReadyToDelete or already be gone -- checked by specific captured ID, so this is
        // unaffected by the new executors' own buckets now live on the same connections. A bucket
        // that no longer exists (already destroyed) counts as resolved too: "stillExisting" only
        // counts the ones still present, so it naturally excludes anything already gone.
        var originalBucketIds = DrainModeTestState.OriginalBucketIds;
        originalBucketIds.Should().NotBeEmpty("Phase2 must have captured Phase1's original bucket IDs");

        await PollingWaitUtil.WaitUntilAsync(OldBucketsResolvedTimeout,
            async () =>
            {
                var stillExisting = await api.GetBucketCountAsync(ClusterId, bucketIds: originalBucketIds);
                var readyToDelete = await api.GetBucketCountAsync(ClusterId, bucketIds: originalBucketIds, status: ReadyToDeleteStatus);
                return stillExisting == readyToDelete;
            },
            "every one of Phase1's original buckets to have reached ReadyToDelete (or already been destroyed)",
            pollInterval: TimeSpan.FromSeconds(30));
    }
}
