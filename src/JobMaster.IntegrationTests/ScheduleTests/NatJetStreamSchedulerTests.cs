using JobMaster.IntegrationTests.Fixtures.SchedulerFixture.NatJetStream;
using Xunit.Abstractions;

namespace JobMaster.IntegrationTests.ScheduleTests;

[Collection("NatJetStreamScheduler")]
public class NatJetStreamSchedulerTests : JobMasterSchedulerTestsBase<NatJetStreamSchedulerFixture>
{
    public NatJetStreamSchedulerTests(NatJetStreamSchedulerFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    
    [Theory]
    [InlineData(50, false, 2)]
    // 250 jobs
    [InlineData(250, false, 2)]
    [InlineData(250, true, 4)]
    // 1000 jobs
    [InlineData(1000, false, 4)]
    [InlineData(1000, true, 6)]
    // 2500 jobs
    [InlineData(2500, false, 6)]
    [InlineData(2500, true, 10)]
    // 5000 jobs
    [InlineData(5000, false, 8)]
    [InlineData(5000, true, 17)]
    // 10000 jobs
    [InlineData(10000, false, 12)]
    [InlineData(10000, true, 25)]
    // 20000 jobs
    [InlineData(20000, false, 20)]
    [InlineData(20000, true, 30)]
    public async Task SchedulerTest(int qtyJobs, bool scheduleAfter, int timeoutInMinutes)
    {
        await RunExecutionTest(qtyJobs, scheduleAfter, timeoutInMinutes, scheduleParallelLimit: 5000);
    }

    [Theory]
    [InlineData("TimeSpanInterval", "00:00:05", 60, 12, 2, 5)]   // Every 5 seconds for 1 minute
    [InlineData("TimeSpanInterval", "00:00:10", 120, 12, 2, 10)]  // Every 10 seconds for 2 minutes
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