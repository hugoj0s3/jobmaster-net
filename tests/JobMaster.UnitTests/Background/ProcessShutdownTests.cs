using System.Collections.Concurrent;
using FluentAssertions;
using JobMaster.Sdk.Background;

namespace JobMaster.UnitTests.Background;

/// <summary>
/// <see cref="ProcessShutdown"/>: the workers stop before the loggers' final flush (so their shutdown logs are
/// written), once, and a failing step never skips the flushes.
/// </summary>
public class ProcessShutdownTests
{
    [Fact]
    public void Run_RunsEveryStepToTheEnd_BeforeAnyFinalFlush_AndOnlyOnce()
    {
        var sut = new ProcessShutdown();
        var order = new ConcurrentQueue<string>();
        sut.OnFinalFlush(() => order.Enqueue("flush"));
        sut.OnShutdown(async () =>
        {
            await Task.Delay(100);
            order.Enqueue("step");
        });

        sut.Run();
        sut.Run();

        order.Should().Equal("step", "flush");
    }

    [Fact]
    public void Run_WhenAStepThrows_StillRunsTheOtherSteps_AndTheFlushes()
    {
        var sut = new ProcessShutdown();
        var order = new ConcurrentQueue<string>();
        sut.OnShutdown(() => throw new InvalidOperationException("boom"));
        sut.OnShutdown(() =>
        {
            order.Enqueue("step");
            return Task.CompletedTask;
        });
        sut.OnFinalFlush(() => throw new InvalidOperationException("boom"));
        sut.OnFinalFlush(() => order.Enqueue("flush"));

        var act = () => sut.Run();

        act.Should().NotThrow();
        order.Should().Equal("step", "flush");
    }

    [Fact]
    public void Run_SkipsARemovedFinalFlush()
    {
        var sut = new ProcessShutdown();
        var flushed = 0;
        Action flush = () => flushed++;
        sut.OnFinalFlush(flush);
        sut.RemoveFinalFlush(flush);

        sut.Run();

        flushed.Should().Be(0);
    }
}
