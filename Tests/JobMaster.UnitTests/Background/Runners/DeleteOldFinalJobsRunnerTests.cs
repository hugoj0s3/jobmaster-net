using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background.Runners.CleanUpData;
using JobMaster.Sdk.Utils;
using Moq;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="DeleteOldFinalJobsRunner"/>.
/// Covers: skip when no config or TTL is null, lock-contention skip, purging finalized jobs
/// older than the TTL while preserving recent ones, and no-op when nothing is eligible.
/// </summary>
public class DeleteOldFinalJobsRunnerTests
{
    // ── helpers ────────────────────────────────────────────────────────────────

    private static ClusterConfigurationModel ConfigWithTtl(TimeSpan ttl)
        => new("test-cluster") { DataRetentionTtl = ttl };

    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenNoClusterConfig_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        // Config is null by default in FakeClusterConfigService.

        var runner = new DeleteOldFinalJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
        f.JobsRepository.Jobs.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenDataRetentionTtlIsNull_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = new ClusterConfigurationModel("test-cluster")
        {
            DataRetentionTtl = TimeSpan.Zero,
        };

        var runner = new DeleteOldFinalJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenLockerTaken_ShouldReturnLocked()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ConfigWithTtl(TimeSpan.FromDays(7));
        f.Locker.BlockAllLocks = true;

        // Add a job that would otherwise be purged.
        f.JobsRepository.Jobs.Add(RunnerFixture.FinalizedJob(DateTime.UtcNow.AddDays(-30)));

        var runner = new DeleteOldFinalJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Locked);
        f.JobsRepository.Jobs.Should().HaveCount(1); // not deleted
    }

    [Fact]
    public async Task OnTickAsync_WhenFinalJobsOlderThanTtl_ShouldPurgeThem()
    {
        var f = RunnerFixture.Create();
        var ttl = TimeSpan.FromDays(7);
        f.ClusterConfig.Config = ConfigWithTtl(ttl);

        // Two old finalized jobs — both should be purged.
        f.JobsRepository.Jobs.Add(RunnerFixture.FinalizedJob(DateTime.UtcNow.AddDays(-30)));
        f.JobsRepository.Jobs.Add(RunnerFixture.FinalizedJob(DateTime.UtcNow.AddDays(-10)));

        // One recent finalized job — within TTL, should NOT be purged.
        f.JobsRepository.Jobs.Add(RunnerFixture.FinalizedJob(DateTime.UtcNow.AddDays(-1)));

        var runner = new DeleteOldFinalJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.JobsRepository.Jobs.Should().HaveCount(1);
        f.JobsRepository.Jobs.Single().FinalizedAt.Should().BeCloseTo(DateTime.UtcNow.AddDays(-1), TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task OnTickAsync_WhenNoJobsToDelete_ShouldReturnSuccess()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ConfigWithTtl(TimeSpan.FromDays(7));
        // No jobs at all.

        var runner = new DeleteOldFinalJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
    }

    [Fact]
    public async Task OnTickAsync_WhenNoArchiveTarget_ShouldAlsoDeletePurgedJobs_JobExecutionCategoryLogs()
    {
        // No archive target configured -- jobs are purged directly (PurgeFinalizedAsync), but their
        // JobExecution-category logs must still be cleaned up here, since DeleteOldLogsRunner deliberately
        // never touches that category (see DeleteOldLogsRunnerTests).
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ConfigWithTtl(TimeSpan.FromDays(7));

        var oldJob = RunnerFixture.FinalizedJob(DateTime.UtcNow.AddDays(-30));
        f.JobsRepository.Jobs.Add(oldJob);

        var jobExecutionLog = new LogItem
        {
            ClusterId = f.ClusterId,
            Id = JobMasterRandomUtil.NewGuid7(),
            Level = JobMasterLogLevel.Error,
            Message = "test",
            Category = JobMasterLogCategory.JobExecution,
            ReferenceId = oldJob.Id.ToString("N"), // matches JobMasterLoggerExtensions' real ReferenceId format
            TimestampUtc = DateTime.UtcNow.AddDays(-30),
        };
        var unrelatedLog = new LogItem
        {
            ClusterId = f.ClusterId,
            Id = JobMasterRandomUtil.NewGuid7(),
            Level = JobMasterLogLevel.Info,
            Message = "unrelated",
            Category = JobMasterLogCategory.Cluster,
            TimestampUtc = DateTime.UtcNow.AddDays(-30),
        };
        f.LogsRepository.Logs.Add(jobExecutionLog);
        f.LogsRepository.Logs.Add(unrelatedLog);

        var runner = new DeleteOldFinalJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.JobsRepository.Jobs.Should().BeEmpty();
        f.LogsRepository.Logs.Should().ContainSingle().Which.Should().Be(unrelatedLog);
    }

    [Fact]
    public async Task OnTickAsync_WhenArchiving_ShouldCopyJobExecutionsAndLogs_ThenPurgeLocally()
    {
        var f = RunnerFixture.Create();
        var archiveClusterId = $"archive-{JobMasterRandomUtil.NewGuid4():N}";
        f.ClusterConfig.Config = ConfigWithTtl(TimeSpan.FromDays(7));
        f.ClusterConfig.Config.TargetArchivedClusterId = archiveClusterId;

        var oldJob = RunnerFixture.FinalizedJob(DateTime.UtcNow.AddDays(-30));
        f.JobsRepository.Jobs.Add(oldJob);

        var execution = new JobExecution(f.ClusterId) { JobId = oldJob.Id };
        f.JobsRepository.JobExecutions.Add(execution);

        var jobExecutionLog = new LogItem
        {
            ClusterId = f.ClusterId,
            Id = JobMasterRandomUtil.NewGuid7(),
            Level = JobMasterLogLevel.Error,
            Message = "test",
            Category = JobMasterLogCategory.JobExecution,
            ReferenceId = oldJob.Id.ToString("N"), // matches JobMasterLoggerExtensions' real ReferenceId format
            TimestampUtc = DateTime.UtcNow.AddDays(-30),
        };
        f.LogsRepository.Logs.Add(jobExecutionLog);

        IList<JobRawModel>? receivedJobs = null;
        IList<JobExecution>? receivedExecutions = null;
        IList<LogItem>? receivedLogs = null;

        var intakeService = new Mock<IMasterJobIntakeService>(MockBehavior.Strict);
        intakeService
            .Setup(x => x.BulkInsertIfNotExistsAsync(
                It.IsAny<IList<JobRawModel>>(), It.IsAny<IList<JobExecution>>(), It.IsAny<IList<LogItem>>()))
            .Callback<IList<JobRawModel>, IList<JobExecution>, IList<LogItem>>((jobs, executions, logs) =>
            {
                receivedJobs = jobs;
                receivedExecutions = executions;
                receivedLogs = logs;
            })
            .Returns(Task.CompletedTask);

        var archiveFactory = new Mock<IJobMasterClusterAwareComponentFactory>(MockBehavior.Strict);
        archiveFactory.SetupGet(x => x.ClusterId).Returns(archiveClusterId);
        archiveFactory.Setup(x => x.GetComponent<IMasterJobIntakeService>()).Returns(intakeService.Object);
        JobMasterClusterAwareComponentFactories.AddFactory(archiveClusterId, archiveFactory.Object);

        var runner = new DeleteOldFinalJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);

        receivedJobs.Should().ContainSingle(j => j.Id == oldJob.Id);
        receivedExecutions.Should().ContainSingle(e => e.JobId == oldJob.Id);
        receivedLogs.Should().ContainSingle(l => l.Id == jobExecutionLog.Id);

        f.JobsRepository.Jobs.Should().BeEmpty();
        f.LogsRepository.Logs.Should().BeEmpty();
    }
}
