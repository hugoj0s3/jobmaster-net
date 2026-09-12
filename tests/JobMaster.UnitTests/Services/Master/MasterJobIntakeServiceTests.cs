using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Services.Master;
using JobMaster.Sdk.Utils;
using Moq;

namespace JobMaster.UnitTests.Services.Master;

public class MasterJobIntakeServiceTests
{
    [Fact]
    public async Task BulkInsertIfNotExistsAsync_WhenArchiving_ShouldReassignAndCopy_ExecutionsAndLogs_ForNewlyInsertedJobsOnly()
    {
        var clusterId = NewClusterId();
        var repo = new Mock<IMasterJobsRepository>(MockBehavior.Strict);
        var logsRepo = new Mock<IMasterLogsRepository>(MockBehavior.Strict);
        var clusterConfigService = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);
        clusterConfigService.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId) { ClusterMode = ClusterMode.Archived });

        var insertedJob = NewJob(JobMasterJobStatus.Succeeded);
        var skippedJob = NewJob(JobMasterJobStatus.Failed);
        var jobs = new List<JobRawModel> { insertedJob, skippedJob };

        var executionForInserted = new JobExecution("source-cluster") { JobId = insertedJob.Id };
        var executionForSkipped = new JobExecution("source-cluster") { JobId = skippedJob.Id };
        var executions = new List<JobExecution> { executionForInserted, executionForSkipped };

        var logForInserted = new LogItem { ClusterId = "source-cluster", ReferenceId = insertedJob.Id.ToString("N") };
        var logForSkipped = new LogItem { ClusterId = "source-cluster", ReferenceId = skippedJob.Id.ToString("N") };
        var logs = new List<LogItem> { logForInserted, logForSkipped };

        repo.Setup(x => x.BulkInsertIfNotExistsAsync(jobs, executions))
            .ReturnsAsync(new List<Guid> { insertedJob.Id });

        IList<LogItem>? receivedLogs = null;
        logsRepo.Setup(x => x.BulkInsertAsync(It.IsAny<IList<LogItem>>()))
            .Callback<IList<LogItem>>(l => receivedLogs = l)
            .Returns(Task.CompletedTask);

        var sut = new MasterJobIntakeService(CreateClusterConfig(clusterId), repo.Object, logsRepo.Object, clusterConfigService.Object);

        await sut.BulkInsertIfNotExistsAsync(jobs, executions, logs);

        foreach (var job in jobs) job.ClusterId.Should().Be(clusterId);
        foreach (var execution in executions) execution.ClusterId.Should().Be(clusterId);

        receivedLogs.Should().ContainSingle(l => l.Id == logForInserted.Id);
        receivedLogs!.Single().ClusterId.Should().Be(clusterId);
    }

    [Fact]
    public async Task BulkInsertIfNotExistsAsync_WhenNoJobsAreNewlyInserted_ShouldNotInsertAnyLogs()
    {
        var clusterId = NewClusterId();
        var repo = new Mock<IMasterJobsRepository>(MockBehavior.Strict);
        var logsRepo = new Mock<IMasterLogsRepository>(MockBehavior.Strict);
        var clusterConfigService = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);
        clusterConfigService.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId) { ClusterMode = ClusterMode.Archived });

        var job = NewJob(JobMasterJobStatus.Succeeded);
        var jobs = new List<JobRawModel> { job };
        var log = new LogItem { ClusterId = "source-cluster", ReferenceId = job.Id.ToString("N") };

        // Repository reports no jobs actually inserted (all already existed).
        repo.Setup(x => x.BulkInsertIfNotExistsAsync(jobs, It.IsAny<IList<JobExecution>>()))
            .ReturnsAsync(new List<Guid>());

        var sut = new MasterJobIntakeService(CreateClusterConfig(clusterId), repo.Object, logsRepo.Object, clusterConfigService.Object);

        await sut.BulkInsertIfNotExistsAsync(jobs, new List<JobExecution>(), new List<LogItem> { log });

        logsRepo.Verify(x => x.BulkInsertAsync(It.IsAny<IList<LogItem>>()), Times.Never);
    }

    [Fact]
    public async Task BulkInsertIfNotExistsAsync_WhenArchivedModeAndJobNotFinal_ShouldThrow()
    {
        var clusterId = NewClusterId();
        var repo = new Mock<IMasterJobsRepository>(MockBehavior.Strict);
        var logsRepo = new Mock<IMasterLogsRepository>(MockBehavior.Strict);
        var clusterConfigService = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);
        clusterConfigService.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId) { ClusterMode = ClusterMode.Archived });

        var nonFinalJob = NewJob(JobMasterJobStatus.OnMaster);
        var sut = new MasterJobIntakeService(CreateClusterConfig(clusterId), repo.Object, logsRepo.Object, clusterConfigService.Object);

        var act = () => sut.BulkInsertIfNotExistsAsync(new List<JobRawModel> { nonFinalJob }, new List<JobExecution>(), new List<LogItem>());

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task BulkInsertIfNotExistsAsync_WhenActiveModeAndJobNotOnMaster_ShouldThrow()
    {
        var clusterId = NewClusterId();
        var repo = new Mock<IMasterJobsRepository>(MockBehavior.Strict);
        var logsRepo = new Mock<IMasterLogsRepository>(MockBehavior.Strict);
        var clusterConfigService = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);
        clusterConfigService.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId) { ClusterMode = ClusterMode.Active });

        var finalJob = NewJob(JobMasterJobStatus.Succeeded);
        var sut = new MasterJobIntakeService(CreateClusterConfig(clusterId), repo.Object, logsRepo.Object, clusterConfigService.Object);

        var act = () => sut.BulkInsertIfNotExistsAsync(new List<JobRawModel> { finalJob }, new List<JobExecution>(), new List<LogItem>());

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    private static string NewClusterId() => $"c{JobMasterRandomUtil.NewGuid4():N}";

    private static JobMasterClusterConnectionConfig CreateClusterConfig(string clusterId)
        => JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: true);

    private static JobRawModel NewJob(JobMasterJobStatus status) => new("source-cluster")
    {
        Id = JobMasterRandomUtil.NewGuid7(),
        JobDefinitionId = "job-def",
        Status = status,
    };
}
