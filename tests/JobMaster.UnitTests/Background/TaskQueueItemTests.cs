using FluentAssertions;
using JobMaster.Sdk.Background.Runners.JobsExecution;

namespace JobMaster.UnitTests.Background;

/// <summary>
/// <see cref="TaskQueueItem{T}"/> at shutdown: a job still running is aborted and disposed without throwing
/// (a throw here aborts the queue shutdown and skips the flush of waiting jobs back to master).
/// </summary>
public class TaskQueueItemTests
{
    [Fact]
    public async Task AbortThenDispose_WhileTheJobStillRuns_DoesNotThrow_AndTheJobSeesTheCancellation()
    {
        var jobObservedCancel = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = new TaskQueueItem<string>("job-1", "value", TimeSpan.FromMinutes(5), async token =>
        {
            try
            {
                await Task.Delay(Timeout.Infinite, token);
            }
            catch (OperationCanceledException)
            {
                jobObservedCancel.TrySetResult(true);
            }
        });

        item.Start();
        item.Task.IsCompleted.Should().BeFalse();

        var act = () =>
        {
            item.Abort();
            item.Dispose();
        };

        act.Should().NotThrow();
        (await jobObservedCancel.Task.WaitAsync(TimeSpan.FromSeconds(5))).Should().BeTrue();
    }
}
