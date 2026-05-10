using FluentAssertions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="StaticRecurringDefinitionsKeepAliveRunner"/>.
/// Covers: lock contention path, tick with no registered static definitions (only
/// <c>InactivateStaticDefinitionsOlderThanAsync</c> is called), and tick when static
/// definition IDs are registered (<c>BulkUpdateStaticDefinitionLastEnsured</c> is also
/// called). Tests that pre-register IDs use <see cref="StaticRecurringDefinitionIdsKeeper"/>
/// and clean up after themselves.
/// </summary>
public class StaticRecurringDefinitionsKeepAliveRunnerTests : IDisposable
{
    private readonly string _clusterId;

    public StaticRecurringDefinitionsKeepAliveRunnerTests()
    {
        // Each test uses a unique cluster ID to avoid cross-test static state pollution.
        _clusterId = $"c{Guid.NewGuid():N}";
    }

    public void Dispose()
        => StaticRecurringDefinitionIdsKeeper.ClearCluster(_clusterId);

    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenLockAlreadyHeld_ShouldReturnLocked()
    {
        var f = CreateFixtureWithClusterId();
        f.Locker.BlockAllLocks = true;

        var runner = new StaticRecurringDefinitionsKeepAliveRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Locked);
        f.RecurringSchedulesService.BulkUpdateLastEnsuredCalls.Should().BeEmpty();
        f.RecurringSchedulesService.InactivateCallCount.Should().Be(0);
    }

    [Fact]
    public async Task OnTickAsync_WhenNoRegisteredDefinitions_ShouldOnlyCallInactivate()
    {
        var f = CreateFixtureWithClusterId();
        // No IDs registered — BulkUpdate should not be called.

        var runner = new StaticRecurringDefinitionsKeepAliveRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.RecurringSchedulesService.BulkUpdateLastEnsuredCalls.Should().BeEmpty();
        f.RecurringSchedulesService.InactivateCallCount.Should().Be(1);
    }

    [Fact]
    public async Task OnTickAsync_WhenDefinitionsAreRegistered_ShouldCallBulkUpdateAndInactivate()
    {
        var f = CreateFixtureWithClusterId();
        var staticId1 = "static-def-1";
        var staticId2 = "static-def-2";

        StaticRecurringDefinitionIdsKeeper.Add(_clusterId, staticId1);
        StaticRecurringDefinitionIdsKeeper.Add(_clusterId, staticId2);

        var runner = new StaticRecurringDefinitionsKeepAliveRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.RecurringSchedulesService.BulkUpdateLastEnsuredCalls.Should().ContainSingle();
        f.RecurringSchedulesService.BulkUpdateLastEnsuredCalls[0].Ids.Should().Contain(staticId1);
        f.RecurringSchedulesService.BulkUpdateLastEnsuredCalls[0].Ids.Should().Contain(staticId2);
        f.RecurringSchedulesService.InactivateCallCount.Should().Be(1);
    }

    [Fact]
    public async Task OnTickAsync_ReleasesLockSoSubsequentTickCanAcquireIt()
    {
        var f = CreateFixtureWithClusterId();

        var runner = new StaticRecurringDefinitionsKeepAliveRunner(f.Worker.Object);
        await runner.OnTickAsync(CancellationToken.None);
        var result2 = await runner.OnTickAsync(CancellationToken.None);

        result2.Status.Should().Be(TicketResultStatus.Success);
        f.RecurringSchedulesService.InactivateCallCount.Should().Be(2);
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Creates a fixture whose worker mock is wired to <see cref="_clusterId"/> so that
    /// <c>StaticRecurringDefinitionIdsKeeper.GetAll</c> resolves to the right cluster.
    /// </summary>
    private RunnerFixture CreateFixtureWithClusterId()
    {
        // Build a fresh fixture then override its cluster ID so the static keeper resolves
        // IDs registered under _clusterId in this test instance.
        var f = RunnerFixture.Create();

        // Wire a cluster config that matches _clusterId.
        var clusterConnConfig = JobMaster.Sdk.Abstractions.Config.JobMasterClusterConnectionConfig
            .Create(_clusterId, "repo", "conn", isDefault: false);
        clusterConnConfig.MarkAsReady();
        f.Worker.SetupGet(x => x.ClusterConnConfig).Returns(clusterConnConfig);

        return f;
    }
}
