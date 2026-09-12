using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Background.Runners.BucketLifeCycleControl;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="AssignedLostBucketsRunner"/>.
/// Covers: skip when stop is requested, lock contention path, no-op when no lost buckets
/// exist, no-op when no alive workers match the bucket's connection, and successful
/// reassignment when an alive Full-mode worker is available for the bucket's connection.
/// </summary>
public class AssignedLostBucketsRunnerTests
{
    private static AgentWorkerModel AliveFullWorker(string clusterId, string workerId, AgentConnectionId connectionId)
        => new(clusterId)
        {
            Id = workerId,
            LastHeartbeat = DateTime.UtcNow,
            AgentConnectionId = connectionId,
            Mode = AgentWorkerMode.Full,
        };

    private static AgentWorkerModel AliveCoordinatorWorker(string clusterId, string workerId)
        => new(clusterId)
        {
            Id = workerId,
            LastHeartbeat = DateTime.UtcNow,
            AgentConnectionId = null, // Coordinators have no AgentConnectionId.
            Mode = AgentWorkerMode.Coordinator,
        };

    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenStopRequested_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.Worker.SetupGet(x => x.StopRequested).Returns(true);

        var runner = new AssignedLostBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenLockAlreadyHeld_ShouldReturnLocked()
    {
        var f = RunnerFixture.Create();
        f.Locker.BlockAllLocks = true;

        var runner = new AssignedLostBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Locked);
    }

    [Fact]
    public async Task OnTickAsync_WhenNoLostBuckets_ShouldReturnSuccess()
    {
        var f = RunnerFixture.Create();
        // Only active buckets — nothing to reassign.
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, "bucket-1"));

        var runner = new AssignedLostBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Buckets.UpdatedBuckets.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenNoAliveWorkersForConnection_ShouldNotReassignBucket()
    {
        var f = RunnerFixture.Create();
        var connectionId = new AgentConnectionId(f.ClusterId, "fake-agent");

        var lostBucket = new BucketModel(f.ClusterId)
        {
            Id = "lost-bucket",
            Status = BucketStatus.Lost,
            AgentConnectionId = connectionId,
            AgentWorkerId = "dead-worker",
        };
        f.Buckets.Buckets.Add(lostBucket);

        // No workers in the service → nothing to assign to.

        var runner = new AssignedLostBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Buckets.UpdatedBuckets.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenAliveWorkerExistsForConnection_ShouldReassignBucket()
    {
        var f = RunnerFixture.Create();
        var connectionId = new AgentConnectionId(f.ClusterId, "fake-agent");

        var lostBucket = new BucketModel(f.ClusterId)
        {
            Id = "lost-bucket",
            Status = BucketStatus.Lost,
            AgentConnectionId = connectionId,
            AgentWorkerId = "dead-worker",
            LastStatusChangeAt = DateTime.UtcNow.AddSeconds(-30),
        };
        f.Buckets.Buckets.Add(lostBucket);

        var aliveWorker = AliveFullWorker(f.ClusterId, "alive-worker", connectionId);
        f.AgentWorkers.Workers.Add(aliveWorker);

        var runner = new AssignedLostBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        // The bucket was updated (status changed to ReadyToDrain).
        f.Buckets.UpdatedBuckets.Should().ContainSingle(b => b.Id == "lost-bucket");
        f.Buckets.UpdatedBuckets[0].Status.Should().Be(BucketStatus.ReadyToDrain);
    }

    [Fact]
    public async Task OnTickAsync_WhenAliveWorkersIncludeCoordinatorWithNoConnection_ShouldSkipItAndStillReassignToFullWorker()
    {
        var f = RunnerFixture.Create();
        var connectionId = new AgentConnectionId(f.ClusterId, "fake-agent");

        var lostBucket = new BucketModel(f.ClusterId)
        {
            Id = "lost-bucket",
            Status = BucketStatus.Lost,
            AgentConnectionId = connectionId,
            AgentWorkerId = "dead-worker",
            LastStatusChangeAt = DateTime.UtcNow.AddSeconds(-30),
        };
        f.Buckets.Buckets.Add(lostBucket);

        // A Coordinator (null AgentConnectionId) mixed in with a real candidate must not blow up
        // the connection-id comparison and must never be selected as the assignee.
        f.AgentWorkers.Workers.Add(AliveCoordinatorWorker(f.ClusterId, "coordinator-worker"));
        f.AgentWorkers.Workers.Add(AliveFullWorker(f.ClusterId, "alive-worker", connectionId));

        var runner = new AssignedLostBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Buckets.UpdatedBuckets.Should().ContainSingle(b => b.Id == "lost-bucket");
        f.Buckets.UpdatedBuckets[0].Status.Should().Be(BucketStatus.ReadyToDrain);
        f.Buckets.UpdatedBuckets[0].AgentWorkerId.Should().Be("alive-worker");
    }

    [Fact]
    public async Task OnTickAsync_WhenWorkerBelongsToDifferentConnection_ShouldNotReassignBucket()
    {
        var f = RunnerFixture.Create();
        var bucketConnection = new AgentConnectionId(f.ClusterId, "connection-A");
        var workerConnection = new AgentConnectionId(f.ClusterId, "connection-B");

        var lostBucket = new BucketModel(f.ClusterId)
        {
            Id = "lost-bucket",
            Status = BucketStatus.Lost,
            AgentConnectionId = bucketConnection,
            AgentWorkerId = "dead-worker",
        };
        f.Buckets.Buckets.Add(lostBucket);

        // Alive worker is on a different connection.
        f.AgentWorkers.Workers.Add(AliveFullWorker(f.ClusterId, "alive-worker", workerConnection));

        var runner = new AssignedLostBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Buckets.UpdatedBuckets.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenLostBucketIsFallback_ShouldMarkReadyToDeleteWithoutSearchingForWorker()
    {
        var f = RunnerFixture.Create();
        var fallbackConnectionId = new AgentConnectionId(f.ClusterId, JobMasterConstants.MasterFallbackAgentConnName);

        var lostFallbackBucket = new BucketModel(f.ClusterId)
        {
            Id = "lost-fallback-bucket",
            Status = BucketStatus.Lost,
            AgentConnectionId = fallbackConnectionId,
            BucketType = BucketType.Fallback,
            LastStatusChangeAt = DateTime.UtcNow.AddSeconds(-30),
        };
        f.Buckets.Buckets.Add(lostFallbackBucket);

        // No worker could ever match the reserved fallback connection — irrelevant here either way.

        var runner = new AssignedLostBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Buckets.UpdatedBuckets.Should().ContainSingle(b => b.Id == "lost-fallback-bucket");
        f.Buckets.UpdatedBuckets[0].Status.Should().Be(BucketStatus.ReadyToDelete);
    }
}
