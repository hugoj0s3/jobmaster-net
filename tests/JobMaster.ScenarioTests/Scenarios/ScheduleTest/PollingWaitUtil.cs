namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest;

internal static class PollingWaitUtil
{
    private static readonly TimeSpan DefaultPollInterval = TimeSpan.FromSeconds(30);

    /// <param name="describeLastState">Optional: what was observed, appended to the timeout message (e.g. a count vs its target).</param>
    public static async Task WaitUntilAsync(TimeSpan timeout, Func<Task<bool>> condition, string description, TimeSpan? pollInterval = null,
        Func<Task<string>>? describeLastState = null)
    {
        var interval = pollInterval ?? DefaultPollInterval;
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(interval);
        }

        var lastState = describeLastState == null ? string.Empty : " Last state: " + await describeLastState() + ".";
        throw new TimeoutException($"Timed out after {timeout} waiting for {description}.{lastState}");
    }
}
