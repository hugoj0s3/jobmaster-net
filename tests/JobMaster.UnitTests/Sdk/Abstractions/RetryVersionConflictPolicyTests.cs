using FluentAssertions;
using JobMaster.Sdk.Abstractions.Exceptions;

namespace JobMaster.UnitTests.Sdk.Abstractions;

public sealed class RetryVersionConflictPolicyTests
{
    private static readonly RetryVersionConflictPolicy Policy = new(TimeSpan.FromMilliseconds(1), maxRetryCount: 3);

    [Fact]
    public async Task ExecAsync_OnAVersionConflict_RunsTheWholeDelegateAgain()
    {
        var runs = 0;

        var result = await Policy.ExecAsync(async () =>
        {
            await Task.Yield();
            if (++runs < 3) throw Conflict();
            return runs;
        });

        result.Should().Be(3);
    }

    [Fact]
    public async Task ExecAsync_WhenEveryRunConflicts_RethrowsAfterTheRetries()
    {
        var runs = 0;

        var act = () => Policy.ExecAsync<bool>(() =>
        {
            runs++;
            throw Conflict();
        });

        await act.Should().ThrowAsync<JobMasterVersionConflictException>();
        runs.Should().Be(4, "the first run plus 3 retries");
    }

    [Fact]
    public async Task ExecAsync_OnAnyOtherException_DoesNotRetry()
    {
        var runs = 0;

        var act = () => Policy.ExecAsync<bool>(() =>
        {
            runs++;
            throw new InvalidOperationException("not a conflict");
        });

        await act.Should().ThrowAsync<InvalidOperationException>();
        runs.Should().Be(1);
    }

    [Fact]
    public void Exec_OnAVersionConflict_RunsTheWholeDelegateAgain()
    {
        var runs = 0;

        var result = Policy.Exec(() =>
        {
            if (++runs < 2) throw Conflict();
            return runs;
        });

        result.Should().Be(2);
    }

    private static JobMasterVersionConflictException Conflict() => new(Guid.NewGuid(), "Job", "v1");
}
