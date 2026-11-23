using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background.Runners.Migration;
using JobMaster.Sdk.Utils;
using Moq;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="MigrateJobsRunner"/> and <see cref="MigrateRecurringSchedulesRunner"/> on Archived
/// and Migrating clusters: count first, Critical when an archived cluster's late jobs/schedules can't be moved,
/// and the slower interval on Archived. Moving to a registered target is covered by the scenario tests.
/// </summary>
public class MigrateRunnersTests
{
    private const string UnregisteredCluster = "not-registered-in-this-process";

    private static ClusterConfigurationModel Config(ClusterMode mode, string? target = null)
        => new("test") { ClusterMode = mode, TargetActiveClusterId = target };

    private static JobRawModel OnMasterJob()
        => new()
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            Status = JobMasterJobStatus.OnMaster,
            NextPlanExecutionAt = DateTime.UtcNow,
        };

    private static void VerifyLogged(RunnerFixture f, JobMasterLogLevel level, Times times)
        => f.Logger.Verify(x => x.Log(level, It.IsAny<string>(), It.IsAny<JobMasterLogCategory?>(), It.IsAny<string?>(),
            It.IsAny<Exception?>(), It.IsAny<string?>(), It.IsAny<string?>(), It.IsAny<int?>()), times);

    // ── MigrateJobsRunner ──────────────────────────────────────────────────────

    [Fact]
    public async Task Jobs_Archived_NoTarget_NoJobs_SkipsWithoutLogging()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = Config(ClusterMode.Archived);

        var result = await new MigrateJobsRunner(f.Worker.Object).OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
        VerifyLogged(f, JobMasterLogLevel.Critical, Times.Never());
    }

    [Fact]
    public async Task Jobs_Archived_NoTarget_WithJob_LogsCriticalAndKeepsJob()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = Config(ClusterMode.Archived);
        var job = OnMasterJob();
        f.JobsService.Jobs.Add(job);

        var result = await new MigrateJobsRunner(f.Worker.Object).OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
        VerifyLogged(f, JobMasterLogLevel.Critical, Times.Once());
        job.ProcessDeadline.Should().BeNull("the job must not be acquired when it can't be moved");
    }

    [Fact]
    public async Task Jobs_Archived_UnreachableTarget_WithJob_LogsCritical()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = Config(ClusterMode.Archived, UnregisteredCluster);
        f.JobsService.Jobs.Add(OnMasterJob());

        var result = await new MigrateJobsRunner(f.Worker.Object).OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
        VerifyLogged(f, JobMasterLogLevel.Critical, Times.Once());
    }

    [Fact]
    public async Task Jobs_Migrating_UnreachableTarget_WithJob_LogsError()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = Config(ClusterMode.Migrating, UnregisteredCluster);
        f.JobsService.Jobs.Add(OnMasterJob());

        var result = await new MigrateJobsRunner(f.Worker.Object).OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
        VerifyLogged(f, JobMasterLogLevel.Error, Times.Once());
        VerifyLogged(f, JobMasterLogLevel.Critical, Times.Never());
    }

    [Fact]
    public void Jobs_Interval_IsTwoAndHalfMinutesOnArchived_ThirtySecondsOnMigrating()
    {
        var f = RunnerFixture.Create();
        var runner = new MigrateJobsRunner(f.Worker.Object);

        f.ClusterConfig.Config = Config(ClusterMode.Archived);
        runner.SucceedInterval.Should().Be(TimeSpan.FromMinutes(2.5));

        f.ClusterConfig.Config = Config(ClusterMode.Migrating, UnregisteredCluster);
        runner.SucceedInterval.Should().Be(TimeSpan.FromSeconds(30));
    }

    // ── MigrateRecurringSchedulesRunner ────────────────────────────────────────

    [Fact]
    public async Task Schedules_Archived_NoTarget_NoSchedules_SkipsWithoutLogging()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = Config(ClusterMode.Archived);

        var result = await new MigrateRecurringSchedulesRunner(f.Worker.Object).OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
        VerifyLogged(f, JobMasterLogLevel.Critical, Times.Never());
    }

    [Fact]
    public async Task Schedules_Archived_NoTarget_WithSchedule_LogsCritical()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = Config(ClusterMode.Archived);
        f.RecurringSchedulesService.Schedules.Add(RunnerFixture.ActiveSchedule(f.ClusterId));

        var result = await new MigrateRecurringSchedulesRunner(f.Worker.Object).OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
        VerifyLogged(f, JobMasterLogLevel.Critical, Times.Once());
    }

    [Fact]
    public async Task Schedules_Archived_UnreachableTarget_WithSchedule_LogsCritical()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = Config(ClusterMode.Archived, UnregisteredCluster);
        f.RecurringSchedulesService.Schedules.Add(RunnerFixture.ActiveSchedule(f.ClusterId));

        var result = await new MigrateRecurringSchedulesRunner(f.Worker.Object).OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
        VerifyLogged(f, JobMasterLogLevel.Critical, Times.Once());
    }

    [Fact]
    public void Schedules_Interval_IsTwoAndHalfMinutesOnArchived_ThirtySecondsOnMigrating()
    {
        var f = RunnerFixture.Create();
        var runner = new MigrateRecurringSchedulesRunner(f.Worker.Object);

        f.ClusterConfig.Config = Config(ClusterMode.Archived);
        runner.SucceedInterval.Should().Be(TimeSpan.FromMinutes(2.5));

        f.ClusterConfig.Config = Config(ClusterMode.Migrating, UnregisteredCluster);
        runner.SucceedInterval.Should().Be(TimeSpan.FromSeconds(30));
    }
}
