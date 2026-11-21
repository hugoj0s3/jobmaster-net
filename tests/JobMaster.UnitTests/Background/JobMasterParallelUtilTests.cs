using System.Collections.Concurrent;
using FluentAssertions;
using JobMaster.Sdk.Background;

namespace JobMaster.UnitTests.Background;

/// <summary>
/// <see cref="JobMasterParallelUtil.ForEachAsync{TSource}"/> keeps <c>Parallel.ForEachAsync</c>'s cancellation
/// contract on every target (on netstandard2.0 it is a hand-written fallback): once cancelled it starts no new item,
/// waits for the running ones, then throws. Callers rely on that to act only on what didn't run.
/// </summary>
public class JobMasterParallelUtilTests
{
    [Fact]
    public async Task ForEachAsync_WhenCancelled_WaitsForRunningItems_StartsNoNewOne_ThenThrows()
    {
        using var cts = new CancellationTokenSource();
        var started = new ConcurrentBag<int>();
        var running = new ConcurrentBag<Task>();
        var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var loop = JobMasterParallelUtil.ForEachAsync(
            Enumerable.Range(0, 10),
            new ParallelOptions { MaxDegreeOfParallelism = 2, CancellationToken = cts.Token },
            async (i, _) =>
            {
                started.Add(i);
                var work = release.Task;
                running.Add(work);
                await work; // ignores the token on purpose: a running item finishes its work
            });

        // Two items are running and the loop waits for a free slot.
        while (started.Count < 2) await Task.Delay(10);

        cts.Cancel();
        await Task.Delay(200);
        loop.IsCompleted.Should().BeFalse("it must wait for the running items before throwing");

        release.SetResult(true);
        var act = async () => await loop;

        await act.Should().ThrowAsync<OperationCanceledException>();
        started.Should().HaveCount(2, "no item starts after the cancellation");
        running.Should().OnlyContain(t => t.IsCompleted);
    }

    [Fact]
    public async Task ForEachAsync_WhenNotCancelled_RunsEveryItem()
    {
        var done = new ConcurrentBag<int>();

        await JobMasterParallelUtil.ForEachAsync(
            Enumerable.Range(0, 20),
            new ParallelOptions { MaxDegreeOfParallelism = 3 },
            async (i, _) =>
            {
                await Task.Yield();
                done.Add(i);
            });

        done.Should().BeEquivalentTo(Enumerable.Range(0, 20));
    }
}
