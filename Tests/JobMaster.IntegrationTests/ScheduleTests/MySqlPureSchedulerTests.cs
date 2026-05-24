using JobMaster.IntegrationTests.Fixtures.SchedulerFixture.MySqlPure;
using Xunit.Abstractions;

namespace JobMaster.IntegrationTests.ScheduleTests;

[Collection("MySqlPureScheduler")]
[Trait("DB", "MySql")]
public class MySqlPureSchedulerTests : JobMasterSchedulerTestsBase<MySqlPureSchedulerFixture>
{
    public MySqlPureSchedulerTests(MySqlPureSchedulerFixture fixture, ITestOutputHelper output) : base(fixture, output) { }

    // MySQL is much slower than Postgres/SqlServer under load. Root cause needs further investigation.
    // Suspect: the acquire process (AcquireAndFetchAsync) — MySQL requires a 3-step lock pattern
    // (SELECT FOR UPDATE SKIP LOCKED → UPDATE → SELECT by lock id) which may serialize workers under load.
    // All timeouts are +3 min over the equivalent Postgres values.
    [Theory]
    [Trait("TestType", "Scheduler")]
    [InlineData(250, false, 5)]
    [InlineData(250, true, 7)]
    // 1000 jobs
    [InlineData(1000, false, 7)]
    [InlineData(1000, true, 11)]
    // 2500 jobs
    [InlineData(2500, false, 9)]
    [InlineData(2500, true, 13)]
    // 5000 jobs
    [InlineData(5000, false, 18)]
    [InlineData(5000, true, 21)]
    public async Task SchedulerTest(int qtyJobs, bool scheduleAfter, int timeoutInMinutes)
    {
        await RunExecutionTest(qtyJobs, scheduleAfter, timeoutInMinutes);
    }

    [Theory]
    [Trait("TestType", "Recurring")]
    [InlineData("TimeSpanInterval", "00:00:45", 180, 4, 1, 45)]   // Every 45 seconds for 3 minutes
    [InlineData("TimeSpanInterval", "00:01:30", 360, 4, 1, 90)]   // Every 90 seconds for 6 minutes
    [InlineData("TimeSpanInterval", "00:01:00", 300, 5, 1, 60)]   // Every 1 minute for 5 minutes
    public async Task RecurringScheduleTest(
        string expressionTypeId, 
        string expression, 
        int durationSeconds,
        int qtyOfJobsExpected, 
        int discrepancyAllow,
        int frequencySeconds)
    {
        await RunRecurringScheduleTest(
            expressionTypeId, 
            expression, 
            TimeSpan.FromSeconds(durationSeconds),
            qtyOfJobsExpected,
            discrepancyAllow,
            TimeSpan.FromSeconds(frequencySeconds));
    }
}