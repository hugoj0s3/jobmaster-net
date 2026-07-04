using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Background.Runners.BucketLifeCycleControl;
using Moq;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="MarkBucketAsLostRunner"/>.
/// Covers: lock contention path, skipping buckets owned by the current worker, skipping
/// buckets whose worker is still alive, marking buckets whose worker is dead, and marking
/// buckets that have no owning worker registered at all.
/// </summary>
public class MarkBucketAsLostRunnerTests
{
    private const string TestBucketId = "bucket-1";

    private static AgentWorkerModel AliveWorker(string clusterId, string workerId, AgentConnectionId connectionId)
        => new(clusterId)
        {
            Id = workerId,
            LastHeartbeat = DateTime.UtcNow,
            AgentConnectionId = connectionId,
        };

    private static AgentWorkerModel DeadWorker(string clusterId, string workerId, AgentConnectionId connectionId)
        => new(clusterId)
        {
            Id = workerId,
            LastHeartbeat = DateTime.UtcNow.AddHours(-2),
            AgentConnectionId = connectionId,
        };

    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenLockAlreadyHeld_ShouldReturnLocked()
    {
        var f = RunnerFixture.Create();
        f.Locker.BlockAllLocks = true;

        var runner = new MarkBucketAsLostRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Locked);
        f.WorkerClusterOps.Verify(
            x => x.MarkBucketAsLostAsync(It.IsAny<string>()),
            Times.Never);
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketOwnedByCurrentWorker_ShouldNotMarkItAsLost()
    {
        var f = RunnerFixture.Create();
        // Bucket is owned by the current worker — runner must skip it.
        var bucket = new BucketModel(f.ClusterId)
        {
            Id = TestBucketId,
            Status = BucketStatus.Active,
            AgentWorkerId = f.WorkerId,
            AgentConnectionId = new AgentConnectionId(f.ClusterId, "fake-agent"),
        };
        f.Buckets.Buckets.Add(bucket);

        var runner = new MarkBucketAsLostRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.WorkerClusterOps.Verify(
            x => x.MarkBucketAsLostAsync(It.IsAny<string>()),
            Times.Never);
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketIsAlreadyLost_ShouldNotMarkItAgain()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.LostBucket(f.ClusterId, TestBucketId, "other-worker"));

        var runner = new MarkBucketAsLostRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.WorkerClusterOps.Verify(
            x => x.MarkBucketAsLostAsync(It.IsAny<string>()),
            Times.Never);
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketWorkerIsAlive_ShouldNotMarkBucketAsLost()
    {
        var f = RunnerFixture.Create();
        var connectionId = new AgentConnectionId(f.ClusterId, "fake-agent");
        var aliveWorker = AliveWorker(f.ClusterId, "other-worker", connectionId);
        f.AgentWorkers.Workers.Add(aliveWorker);

        var bucket = new BucketModel(f.ClusterId)
        {
            Id = TestBucketId,
            Status = BucketStatus.Active,
            AgentWorkerId = aliveWorker.Id,
            AgentConnectionId = connectionId,
        };
        f.Buckets.Buckets.Add(bucket);

        var runner = new MarkBucketAsLostRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.WorkerClusterOps.Verify(
            x => x.MarkBucketAsLostAsync(It.IsAny<string>()),
            Times.Never);
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketWorkerIsDead_ShouldMarkBucketAsLost()
    {
        var f = RunnerFixture.Create();
        var connectionId = new AgentConnectionId(f.ClusterId, "fake-agent");
        var deadWorker = DeadWorker(f.ClusterId, "dead-worker", connectionId);
        f.AgentWorkers.Workers.Add(deadWorker);

        var bucket = new BucketModel(f.ClusterId)
        {
            Id = TestBucketId,
            Status = BucketStatus.Active,
            AgentWorkerId = deadWorker.Id,
            AgentConnectionId = connectionId,
        };
        f.Buckets.Buckets.Add(bucket);

        f.WorkerClusterOps
            .Setup(x => x.MarkBucketAsLostAsync(It.IsAny<string>()))
            .Returns(Task.CompletedTask);

        var runner = new MarkBucketAsLostRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.WorkerClusterOps.Verify(
            x => x.MarkBucketAsLostAsync(TestBucketId),
            Times.Once);
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketHasNoOwnerRegistered_ShouldMarkBucketAsLost()
    {
        var f = RunnerFixture.Create();
        // Bucket references a worker ID that is not in the workers list.
        var bucket = new BucketModel(f.ClusterId)
        {
            Id = TestBucketId,
            Status = BucketStatus.Active,
            AgentWorkerId = "missing-worker",
            AgentConnectionId = new AgentConnectionId(f.ClusterId, "fake-agent"),
        };
        f.Buckets.Buckets.Add(bucket);

        f.WorkerClusterOps
            .Setup(x => x.MarkBucketAsLostAsync(It.IsAny<string>()))
            .Returns(Task.CompletedTask);

        var runner = new MarkBucketAsLostRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.WorkerClusterOps.Verify(
            x => x.MarkBucketAsLostAsync(TestBucketId),
            Times.Once);
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketHasNullWorkerField_ShouldMarkBucketAsLost()
    {
        var f = RunnerFixture.Create();
        var bucket = new BucketModel(f.ClusterId)
        {
            Id = TestBucketId,
            Status = BucketStatus.Active,
            AgentWorkerId = null,
            AgentConnectionId = new AgentConnectionId(f.ClusterId, "fake-agent"),
        };
        f.Buckets.Buckets.Add(bucket);

        f.WorkerClusterOps
            .Setup(x => x.MarkBucketAsLostAsync(It.IsAny<string>()))
            .Returns(Task.CompletedTask);

        var runner = new MarkBucketAsLostRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.WorkerClusterOps.Verify(
            x => x.MarkBucketAsLostAsync(TestBucketId),
            Times.Once);
    }
}
