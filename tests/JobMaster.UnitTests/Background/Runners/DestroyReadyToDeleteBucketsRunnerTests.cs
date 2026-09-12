using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Background.Runners.BucketLifeCycleControl;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="DestroyReadyToDeleteBucketsRunner"/>.
/// Covers: lock contention path, skipping buckets whose <c>DeletesAt</c> is null or in the
/// future, reverting to Lost when jobs are still present, reverting to Lost when
/// <c>DeletesAt</c> is null on the fresh read, and successfully destroying eligible buckets.
/// </summary>
public class DestroyReadyToDeleteBucketsRunnerTests
{
    /// <summary>
    /// Returns a ReadyToDelete bucket whose <c>DeletesAt</c> is already in the past so the
    /// runner considers it eligible for destruction.
    /// </summary>
    private static BucketModel EligibleBucket(string clusterId, string bucketId)
        => new(clusterId)
        {
            Id = bucketId,
            Status = BucketStatus.ReadyToDelete,
            AgentConnectionId = new AgentConnectionId(clusterId, "fake-agent"),
            AgentWorkerId = null,
            DeletesAt = DateTime.UtcNow.AddMinutes(-1),
        };

    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenLockAlreadyHeld_ShouldReturnLocked()
    {
        var f = RunnerFixture.Create();
        f.Locker.BlockAllLocks = true;

        var runner = new DestroyReadyToDeleteBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Locked);
        f.Buckets.DestroyedBucketIds.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenNoReadyToDeleteBuckets_ShouldReturnSuccess()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, "active-bucket"));

        var runner = new DestroyReadyToDeleteBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Buckets.DestroyedBucketIds.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketDeletesAtIsInFuture_ShouldNotDestroyIt()
    {
        var f = RunnerFixture.Create();
        var bucket = new BucketModel(f.ClusterId)
        {
            Id = "future-bucket",
            Status = BucketStatus.ReadyToDelete,
            AgentConnectionId = new AgentConnectionId(f.ClusterId, "fake-agent"),
            DeletesAt = DateTime.UtcNow.AddMinutes(30),
        };
        f.Buckets.Buckets.Add(bucket);

        var runner = new DestroyReadyToDeleteBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Buckets.DestroyedBucketIds.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketHasNoDeletesAt_ShouldSkipIt()
    {
        var f = RunnerFixture.Create();
        var bucket = new BucketModel(f.ClusterId)
        {
            Id = "no-deletes-at-bucket",
            Status = BucketStatus.ReadyToDelete,
            AgentConnectionId = new AgentConnectionId(f.ClusterId, "fake-agent"),
            DeletesAt = null,
        };
        f.Buckets.Buckets.Add(bucket);

        var runner = new DestroyReadyToDeleteBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        // DeletesAt is null → skipped at the initial filter; no update or destroy.
        f.Buckets.DestroyedBucketIds.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketStillHasJobs_ShouldRevertToLost()
    {
        var f = RunnerFixture.Create();
        var bucket = EligibleBucket(f.ClusterId, "has-jobs-bucket");
        f.Buckets.Buckets.Add(bucket);

        // Dispatcher reports that jobs remain.
        f.JobsDispatcher.HasJobsResult = true;

        var runner = new DestroyReadyToDeleteBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Buckets.DestroyedBucketIds.Should().BeEmpty();
        // Bucket should have been updated to Lost status.
        f.Buckets.UpdatedBuckets.Should().ContainSingle(b => b.Id == "has-jobs-bucket");
        f.Buckets.UpdatedBuckets[0].Status.Should().Be(BucketStatus.Lost);
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketIsEligible_ShouldDestroyIt()
    {
        var f = RunnerFixture.Create();
        var bucket = EligibleBucket(f.ClusterId, "eligible-bucket");
        f.Buckets.Buckets.Add(bucket);

        // No jobs remain.
        f.JobsDispatcher.HasJobsResult = false;

        var runner = new DestroyReadyToDeleteBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Buckets.DestroyedBucketIds.Should().ContainSingle(id => id == "eligible-bucket");
    }

    [Fact]
    public async Task OnTickAsync_WhenFallbackBucketStillHasJobs_ShouldDestroyItAnywayInsteadOfRevertingToLost()
    {
        var f = RunnerFixture.Create();
        var bucket = new BucketModel(f.ClusterId)
        {
            Id = "fallback-has-jobs-bucket",
            Status = BucketStatus.ReadyToDelete,
            BucketType = BucketType.Fallback,
            AgentConnectionId = new AgentConnectionId(f.ClusterId, JobMasterConstants.MasterFallbackAgentConnName),
            DeletesAt = DateTime.UtcNow.AddMinutes(-1),
        };
        f.Buckets.Buckets.Add(bucket);

        // Jobs are still present in the agent-side tables — must not block destruction for Fallback.
        f.JobsDispatcher.HasJobsResult = true;

        var runner = new DestroyReadyToDeleteBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Buckets.DestroyedBucketIds.Should().ContainSingle(id => id == "fallback-has-jobs-bucket");
        f.Buckets.UpdatedBuckets.Should().NotContain(b => b.Id == "fallback-has-jobs-bucket" && b.Status == BucketStatus.Lost);
    }
}
