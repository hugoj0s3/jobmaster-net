using System.Diagnostics;
using FluentAssertions;
using JobMaster.Sdk.Abstractions.Background;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// <see cref="JobMasterRunner"/> stop lifecycle: cleanup (<c>OnStopAsync</c>) runs exactly once, in the runner's own
/// task, and a stop from inside the runner never waits on itself.
/// </summary>
public class JobMasterRunnerLifecycleTests
{
    private sealed class TestRunner : JobMasterRunner
    {
        private readonly bool stopFromTick;
        public int StopCount;
        public readonly TaskCompletionSource<bool> Ticked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TimeSpan? SelfStopElapsed;

        public TestRunner(IJobMasterBackgroundAgentWorker worker, bool stopFromTick)
            : base(worker, bucketAwareLifeCycle: true, useSemaphore: false)
        {
            this.stopFromTick = stopFromTick;
        }

        public override TimeSpan SucceedInterval => TimeSpan.FromMinutes(1);

        public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
        {
            if (stopFromTick)
            {
                var sw = Stopwatch.StartNew();
                await StopAsync();
                SelfStopElapsed = sw.Elapsed;
            }

            Ticked.TrySetResult(true);
            // Long delay: an outside stop lands while the runner waits for its next tick.
            return OnTickResult.Success(TimeSpan.FromMinutes(1));
        }

        public override Task OnStopAsync()
        {
            Interlocked.Increment(ref StopCount);
            return Task.CompletedTask;
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var sw = Stopwatch.StartNew();
        while (!condition() && sw.Elapsed < TimeSpan.FromSeconds(5))
        {
            await Task.Delay(20);
        }
    }

    [Fact]
    public async Task StopAsync_FromOutside_RunsCleanupOnce_AndRemovesTheRunner()
    {
        var f = RunnerFixture.Create();
        var runner = new TestRunner(f.Worker.Object, stopFromTick: false);

        await runner.StartAsync();
        f.Worker.Object.Runners.Snapshot().Should().Contain(runner);
        await runner.Ticked.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var sw = Stopwatch.StartNew();
        await runner.StopAsync();
        await runner.StopAsync();

        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(5), "the runner was cancelled mid-delay and must end promptly");
        runner.StopCount.Should().Be(1);
        f.Worker.Object.Runners.Snapshot().Should().NotContain(runner);
    }

    [Fact]
    public async Task StopAsync_FromOwnTick_DoesNotWaitOnItself_AndRunsCleanupOnce()
    {
        var f = RunnerFixture.Create();
        var runner = new TestRunner(f.Worker.Object, stopFromTick: true);

        await runner.StartAsync();
        await runner.Ticked.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await WaitUntilAsync(() => runner.StopCount > 0);

        runner.SelfStopElapsed.Should().NotBeNull();
        runner.SelfStopElapsed!.Value.Should().BeLessThan(TimeSpan.FromSeconds(2), "waiting on its own task would take the 10 s stop timeout");
        runner.StopCount.Should().Be(1);
        f.Worker.Object.Runners.Snapshot().Should().NotContain(runner);
    }
}
