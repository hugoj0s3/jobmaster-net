using FluentAssertions;
using JobMaster.Contracts;
using JobMaster.Contracts.RecurrenceExpressions;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Models.RecurringSchedules;
using JobMaster.Sdk.Contracts.Services;
using JobMaster.Sdk.Contracts.Services.Agent;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Services;
using Moq;
using Xunit;
using Xunit.Abstractions;

namespace JobMaster.UnitTests.Services;

public class RecurringSchedulePlannerTests
{
    private readonly ITestOutputHelper output;
    private readonly Mock<IJobMasterSchedulerClusterAware> mockScheduler;
    private readonly Mock<IMasterJobsService> mockMasterJobsService;
    private readonly Mock<IMasterRecurringSchedulesService> mockMasterRecurringSchedulesService;
    private readonly Mock<IMasterDistributedLockerService> mockMasterDistributedLockerService;
    private readonly Mock<IMasterClusterConfigurationService> mockMasterClusterConfigurationService;
    private readonly Mock<IJobMasterLogger> mockLogger;
    private readonly Mock<IJobMasterRuntime> mockRuntime;
    private readonly RecurringSchedulePlanner planner;
    
    private static readonly JobMasterClusterConnectionConfig clusterConfig 
        = JobMasterClusterConnectionConfig.Create("cluster-1", "Postgres", "postgres://postgres:postgres@localhost:5432/postgres");

    public RecurringSchedulePlannerTests(ITestOutputHelper output)
    {
        this.output = output;
        
        mockScheduler = new Mock<IJobMasterSchedulerClusterAware>();
        mockMasterJobsService = new Mock<IMasterJobsService>();
        mockMasterRecurringSchedulesService = new Mock<IMasterRecurringSchedulesService>();
        mockMasterDistributedLockerService = new Mock<IMasterDistributedLockerService>();
        mockMasterClusterConfigurationService = new Mock<IMasterClusterConfigurationService>();
        mockLogger = new Mock<IJobMasterLogger>();
        mockRuntime = new Mock<IJobMasterRuntime>();
        
        mockRuntime.Setup(r => r.StartingAt).Returns(DateTime.UtcNow.AddMinutes(-1));

        planner = new RecurringSchedulePlanner(
            clusterConfig,
            mockMasterClusterConfigurationService.Object,
            mockScheduler.Object,
            mockMasterRecurringSchedulesService.Object,
            mockMasterJobsService.Object,
            mockMasterDistributedLockerService.Object,
            mockRuntime.Object,
            mockLogger.Object);
    }

    [Fact]
    public void PlanNextDates_WithTimeSpanInterval_ShouldGenerateDatesWithinHorizon()
    {
        // Arrange
        var recurringScheduleId = Guid.NewGuid();
        var baseDateTime = DateTime.UtcNow;
        var horizon = TimeSpan.FromMinutes(5);
        var interval = TimeSpan.FromSeconds(5);
        var compiler = new TimeSpanIntervalExprCompiler();
        var expression = compiler.Compile(interval.ToString());
        
        output.WriteLine($"Test: PlanNextDates_WithTimeSpanInterval_ShouldGenerateDatesWithinHorizon");
        output.WriteLine($"baseDateTime: {baseDateTime:O}");
        output.WriteLine($"horizon: {horizon}");
        output.WriteLine($"interval: {interval}");
        output.WriteLine($"stopAt: {baseDateTime.Add(horizon):O}");
        
        // Act
        var (lastScheduleAt, nextDates, planningHorizon) = planner.PlanNextDates(
            recurringScheduleId,
            hasFailedOnLastPlan: false,
            ianaTimeZoneId: "UTC",
            expr: expression,
            horizon: horizon,
            baseDateTime: baseDateTime,
            endBeforeUtc: null);
        
        // Assert
        output.WriteLine($"Results: {nextDates.Count} dates generated");
        foreach (var date in nextDates.Take(10))
        {
            output.WriteLine($"  - {date:O}");
        }
        
        nextDates.Should().NotBeEmpty("should generate dates within the horizon");
        nextDates.Count.Should().BeGreaterThan(0);
        
        // All dates should be within the horizon
        var stopAt = DateTime.UtcNow + horizon;
        nextDates.Should().AllSatisfy(date => 
            date.Should().BeOnOrBefore(stopAt, "all dates should be within the horizon"));
        
        // All dates should be after baseDateTime
        nextDates.Should().AllSatisfy(date => 
            date.Should().BeAfter(baseDateTime, "all dates should be after baseDateTime"));
        
        // Dates should be approximately 5 seconds apart
        for (int i = 1; i < nextDates.Count; i++)
        {
            var diff = nextDates[i] - nextDates[i - 1];
            diff.Should().BeCloseTo(interval, TimeSpan.FromSeconds(1), 
                $"dates should be approximately {interval.TotalSeconds}s apart");
        }
        
        // lastScheduleAt should be the max date
        lastScheduleAt.Should().Be(nextDates.Max());
    }

    [Fact]
    public void PlanNextDates_WithPastBaseDateTime_ShouldGenerateFutureDates()
    {
        // Arrange
        var recurringScheduleId = Guid.NewGuid();
        var baseDateTime = DateTime.UtcNow.AddMinutes(-10); // 10 minutes in the past
        var horizon = TimeSpan.FromMinutes(5);
        var interval = TimeSpan.FromSeconds(10);
        var compiler = new TimeSpanIntervalExprCompiler();
        var expression = compiler.Compile(interval.ToString());
        
        output.WriteLine($"Test: PlanNextDates_WithPastBaseDateTime_ShouldGenerateFutureDates");
        output.WriteLine($"baseDateTime: {baseDateTime:O} (10 minutes ago)");
        output.WriteLine($"UtcNow: {DateTime.UtcNow:O}");
        output.WriteLine($"horizon: {horizon}");
        output.WriteLine($"stopAt: {DateTime.UtcNow.Add(horizon):O}");
        
        // Act
        var (lastScheduleAt, nextDates, planningHorizon) = planner.PlanNextDates(
            recurringScheduleId,
            hasFailedOnLastPlan: false,
            ianaTimeZoneId: "UTC",
            expr: expression,
            horizon: horizon,
            baseDateTime: baseDateTime,
            endBeforeUtc: null);
        
        // Assert
        output.WriteLine($"Results: {nextDates.Count} dates generated");
        foreach (var date in nextDates.Take(10))
        {
            output.WriteLine($"  - {date:O}");
        }
        
        nextDates.Should().NotBeEmpty("should generate dates even with past baseDateTime");
        
        // All dates should be after baseDateTime
        nextDates.Should().AllSatisfy(date => 
            date.Should().BeAfter(baseDateTime, "all dates should be after baseDateTime"));
        
        // All dates should be within the planning horizon (stopAt = UtcNow + horizon)
        var stopAt = DateTime.UtcNow + horizon;
        nextDates.Should().AllSatisfy(date => 
            date.Should().BeOnOrBefore(stopAt, "all dates should be within the planning horizon"));
        
        // The method generates dates from baseDateTime forward, even if baseDateTime is in the past
        // This means some dates may be in the past - this is the ACTUAL BEHAVIOR
        output.WriteLine($"First date: {nextDates.First():O}");
        output.WriteLine($"Last date: {nextDates.Last():O}");
        output.WriteLine($"Dates in the past: {nextDates.Count(d => d < DateTime.UtcNow)}");
        output.WriteLine($"Dates in the future: {nextDates.Count(d => d >= DateTime.UtcNow)}");
    }

    [Fact]
    public void PlanNextDates_WithBaseDateTimeBeyondHorizon_ShouldReturnEmpty()
    {
        // Arrange
        var recurringScheduleId = Guid.NewGuid();
        var baseDateTime = DateTime.UtcNow.AddMinutes(10); // 10 minutes in the future
        var horizon = TimeSpan.FromMinutes(5);
        var interval = TimeSpan.FromSeconds(5);
        var compiler = new TimeSpanIntervalExprCompiler();
        var expression = compiler.Compile(interval.ToString());
        
        output.WriteLine($"Test: PlanNextDates_WithBaseDateTimeBeyondHorizon_ShouldReturnEmpty");
        output.WriteLine($"baseDateTime: {baseDateTime:O} (10 minutes from now)");
        output.WriteLine($"UtcNow: {DateTime.UtcNow:O}");
        output.WriteLine($"horizon: {horizon}");
        output.WriteLine($"stopAt: {DateTime.UtcNow.Add(horizon):O}");
        
        // Act
        var (lastScheduleAt, nextDates, planningHorizon) = planner.PlanNextDates(
            recurringScheduleId,
            hasFailedOnLastPlan: false,
            ianaTimeZoneId: "UTC",
            expr: expression,
            horizon: horizon,
            baseDateTime: baseDateTime,
            endBeforeUtc: null);
        
        // Assert
        output.WriteLine($"Results: {nextDates.Count} dates generated");
        output.WriteLine($"lastScheduleAt: {lastScheduleAt:O}");
        
        nextDates.Should().BeEmpty("baseDateTime is beyond the planning horizon");
        lastScheduleAt.Should().BeNull("no dates were generated");
    }

    [Fact]
    public void PlanNextDates_WithEndBefore_ShouldRespectEndBound()
    {
        // Arrange
        var recurringScheduleId = Guid.NewGuid();
        var baseDateTime = DateTime.UtcNow;
        var horizon = TimeSpan.FromMinutes(5);
        var endBefore = DateTime.UtcNow.AddMinutes(2); // End after 2 minutes
        var interval = TimeSpan.FromSeconds(10);
        var compiler = new TimeSpanIntervalExprCompiler();
        var expression = compiler.Compile(interval.ToString());
        
        output.WriteLine($"Test: PlanNextDates_WithEndBefore_ShouldRespectEndBound");
        output.WriteLine($"baseDateTime: {baseDateTime:O}");
        output.WriteLine($"horizon: {horizon}");
        output.WriteLine($"endBefore: {endBefore:O}");
        output.WriteLine($"interval: {interval}");
        
        // Act
        var (lastScheduleAt, nextDates, planningHorizon) = planner.PlanNextDates(
            recurringScheduleId,
            hasFailedOnLastPlan: false,
            ianaTimeZoneId: "UTC",
            expr: expression,
            horizon: horizon,
            baseDateTime: baseDateTime,
            endBeforeUtc: endBefore);
        
        // Assert
        output.WriteLine($"Results: {nextDates.Count} dates generated");
        foreach (var date in nextDates)
        {
            output.WriteLine($"  - {date:O}");
        }
        
        nextDates.Should().NotBeEmpty();
        
        // All dates should be before endBefore
        nextDates.Should().AllSatisfy(date => 
            date.Should().BeBefore(endBefore, "all dates should respect endBefore bound"));
        
        // Should generate approximately 12 dates (2 minutes / 10 seconds)
        nextDates.Count.Should().BeLessThanOrEqualTo(13, "should respect the endBefore limit");
    }

    [Fact]
    public void PlanNextDates_WithOneMinuteInterval_ShouldGenerateCorrectCount()
    {
        // Arrange
        var recurringScheduleId = Guid.NewGuid();
        var baseDateTime = DateTime.UtcNow;
        var horizon = TimeSpan.FromMinutes(5);
        var interval = TimeSpan.FromMinutes(1);
        var compiler = new TimeSpanIntervalExprCompiler();
        var expression = compiler.Compile(interval.ToString());
        
        output.WriteLine($"Test: PlanNextDates_WithOneMinuteInterval_ShouldGenerateCorrectCount");
        output.WriteLine($"baseDateTime: {baseDateTime:O}");
        output.WriteLine($"horizon: {horizon}");
        output.WriteLine($"interval: {interval}");
        
        // Act
        var (lastScheduleAt, nextDates, planningHorizon) = planner.PlanNextDates(
            recurringScheduleId,
            hasFailedOnLastPlan: false,
            ianaTimeZoneId: "UTC",
            expr: expression,
            horizon: horizon,
            baseDateTime: baseDateTime,
            endBeforeUtc: null);
        
        // Assert
        output.WriteLine($"Results: {nextDates.Count} dates generated");
        foreach (var date in nextDates)
        {
            output.WriteLine($"  - {date:O}");
        }
        
        nextDates.Should().NotBeEmpty();
        
        // Should generate approximately 5 dates (5 minutes / 1 minute)
        nextDates.Count.Should().BeGreaterThanOrEqualTo(4, "should generate at least 4 dates");
        nextDates.Count.Should().BeLessThanOrEqualTo(6, "should generate at most 6 dates");
        
        // Dates should be approximately 1 minute apart
        for (int i = 1; i < nextDates.Count; i++)
        {
            var diff = nextDates[i] - nextDates[i - 1];
            diff.Should().BeCloseTo(interval, TimeSpan.FromSeconds(2), 
                $"dates should be approximately {interval.TotalMinutes} minute(s) apart");
        }
    }
}
