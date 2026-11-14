using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Background.Runners.JobsExecution;
using JobMaster.Sdk.Utils;

namespace JobMaster.UnitTests.Background.Runners;

public class JobExecutionMessagesTests
{
    [Fact]
    public void HandlerFailed_SaysWhatHappenedThenWhatsNext_ThenPointsToTheLogs()
    {
        var message = JobExecutionMessages.HandlerFailed(120, new InvalidOperationException("Payment gateway unavailable."),
            "Retrying in 00:02:00 (RetryDelays).");

        message.Should().Be(
            "Failed after 120 ms: InvalidOperationException: Payment gateway unavailable. " +
            "Retrying in 00:02:00 (RetryDelays). See the job's logs for the full error.");
    }

    [Fact]
    public void HandlerFailed_LongMultiLineExceptionMessage_IsKeptToOneShortLine()
    {
        var longText = "line one\r\nline two " + new string('x', 1000);

        var message = JobExecutionMessages.HandlerFailed(5, new Exception(longText), "Reached max retries (3).");

        message.Should().NotContain("\n").And.NotContain("\r");
        message.Should().Contain("line one  line two");
        message.Should().Contain("…");
        message.Length.Should().BeLessThan(700);
    }

    [Fact]
    public void HandlerFailed_EmptyExceptionMessage_UsesTheTypeName()
    {
        var message = JobExecutionMessages.HandlerFailed(5, new CustomEmptyException(), "Reached max retries (3).");

        message.Should().StartWith("Failed after 5 ms: CustomEmptyException. Reached max retries (3).");
    }

    [Fact]
    public void TimedOut_HasNoLogPointer_SinceThereIsNoStackTrace()
    {
        var message = JobExecutionMessages.TimedOut(61250, TimeSpan.FromMinutes(1), "Retrying in 00:00:30 (RetryDelays).");

        message.Should().Be("Timed out after 61250 ms (timeout 00:01:00). Retrying in 00:00:30 (RetryDelays).");
    }

    [Fact]
    public void RecurringMessages_UseBase64Ids()
    {
        var scheduleId = JobMasterRandomUtil.NewGuid7();

        JobExecutionMessages.RecurringOverlap(scheduleId).Should().Contain(scheduleId.ToBase64()).And.NotContain(scheduleId.ToString());
        JobExecutionMessages.RecurringScheduleNotFound(scheduleId).Should().Contain(scheduleId.ToBase64()).And.NotContain(scheduleId.ToString());
    }

    [Fact]
    public void ConcurrentChange_NamesTheStatus_AndPointsToTheLogs()
    {
        JobExecutionMessages.ConcurrentChange(JobMasterJobStatus.Processing)
            .Should().Contain("now Processing").And.EndWith("See the job's logs for the full error.");
    }

    private sealed class CustomEmptyException : Exception
    {
        public CustomEmptyException() : base(string.Empty)
        {
        }
    }
}
