namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest;

internal static class PollingWaitUtil
{
    private static readonly TimeSpan DefaultPollInterval = TimeSpan.FromSeconds(30);

    public static async Task WaitUntilAsync(TimeSpan timeout, Func<Task<bool>> condition, string description, TimeSpan? pollInterval = null)
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

        throw new TimeoutException($"Timed out after {timeout} waiting for {description}.");
    }
}
