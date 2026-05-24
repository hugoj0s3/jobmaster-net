using JobMaster.IntegrationTests.Fixtures.SchedulerFixture.PostgresPure;
using Xunit.Abstractions;

namespace JobMaster.IntegrationTests.ScheduleTests;

[Collection("PostgresPureScheduler")]
[Trait("DB", "Postgres")]
public class PostgresPureSchedulerTests : JobMasterSchedulerTestsBase<PostgresPureSchedulerFixture>
{
    public PostgresPureSchedulerTests(PostgresPureSchedulerFixture fixture, ITestOutputHelper output) : base(fixture, output) { }

    [Theory]
    [Trait("TestType", "Scheduler")]
    // 250 jobs
    [InlineData(250, false, 2)]
    [InlineData(250, true, 4)]
    // 1000 jobs
    [InlineData(1000, false, 4)]
    [InlineData(1000, true, 8)]
    // 2500 jobs
    [InlineData(2500, false, 6)]
    [InlineData(2500, true, 10)]
    // 5000 jobs
    [InlineData(5000, false, 12)]
    [InlineData(5000, true, 18)]
    public async Task SchedulerTest(int qtyJobs, bool scheduleAfter, int timeoutInMinutes)
    {
        await RunExecutionTest(qtyJobs, scheduleAfter, timeoutInMinutes, scheduleParallelLimit: 50);
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