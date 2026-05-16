using FluentAssertions;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Services.Master;
using JobMaster.Sdk.Utils;

namespace JobMaster.UnitTests.Services.Master;

public class JobMasterLoggerTests
{
    [Fact]
    public async Task Log_WhenMaxBatchSizeReached_ShouldFlushWithBulkInsertAsync()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var tcs = new TaskCompletionSource<IList<LogItem>>(TaskCreationOptions.RunContinuationsAsynchronously);

        var repo = new Mock<IMasterLogsRepository>(MockBehavior.Strict);
        repo.Setup(x => x.BulkInsertAsync(It.IsAny<IList<LogItem>>()))
            .Returns<IList<LogItem>>(items =>
            {
                tcs.TrySetResult(items);
                return Task.CompletedTask;
            });

        using var sut = new JobMasterLogger(clusterConfig, repo.Object);

        for (var i = 0; i < 100; i++)
        {
            sut.Log(JobMasterLogLevel.Info, $"m{i}", JobMasterLogCategory.Job, "s");
        }

        var completed = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(2)));
        completed.Should().Be(tcs.Task);

        var flushed = await tcs.Task;
        flushed.Should().NotBeNull();
        flushed.Count.Should().BeGreaterThan(0);
        flushed.Count.Should().BeLessThanOrEqualTo(100);

        repo.Verify(x => x.BulkInsertAsync(It.IsAny<IList<LogItem>>()), Times.AtLeastOnce);
    }

    [Fact]
    public async Task QueryAsync_WhenCriteriaProvided_ShouldDelegateToRepo()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var ts = new DateTime(2025, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var expected = new List<LogItem>
        {
            new LogItem
            {
                ClusterId = clusterId,
                Id = JobMasterRandomUtil.NewGuid4(),
                Level = JobMasterLogLevel.Error,
                Message = "hello",
                Category = JobMasterLogCategory.Job,
                ReferenceId = "sid",
                TimestampUtc = ts,
                Host = "h",
                SourceMember = "DequeueSavePendingRecur",
                SourceFile = "AgentJobsDispatcherRepository.cs",
                SourceLine = 93,
            }
        };

        LogItemQueryCriteria? captured = null;

        var repo = new Mock<IMasterLogsRepository>(MockBehavior.Strict);
        repo
            .Setup(x => x.QueryAsync(It.IsAny<LogItemQueryCriteria>()))
            .Callback<LogItemQueryCriteria>(c => captured = c)
            .ReturnsAsync(expected);

        using var sut = new JobMasterLogger(clusterConfig, repo.Object);

        var from = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var to = new DateTime(2025, 1, 3, 0, 0, 0, DateTimeKind.Utc);

        var result = await sut.QueryAsync(new LogItemQueryCriteria
        {
            FromTimestamp = from,
            ToTimestamp = to,
            Level = JobMasterLogLevel.Error,
            Keyword = "hello",
            Category = JobMasterLogCategory.Job,
            ReferenceId = "sid"
        });

        captured.Should().NotBeNull();
        captured!.FromTimestamp.Should().Be(from);
        captured.ToTimestamp.Should().Be(to);
        captured.Level.Should().Be(JobMasterLogLevel.Error);
        captured.Keyword.Should().Be("hello");
        captured.Category.Should().Be(JobMasterLogCategory.Job);
        captured.ReferenceId.Should().Be("sid");

        result.Should().HaveCount(1);
        result[0].ReferenceId.Should().Be("sid");
        result[0].Category.Should().Be(JobMasterLogCategory.Job);
        result[0].Level.Should().Be(JobMasterLogLevel.Error);
        result[0].Message.Should().Be("hello");
        result[0].SourceMember.Should().Be("DequeueSavePendingRecur");
        result[0].SourceFile.Should().Be("AgentJobsDispatcherRepository.cs");
        result[0].SourceLine.Should().Be(93);

        repo.Verify(x => x.QueryAsync(It.IsAny<LogItemQueryCriteria>()), Times.Once);
    }

    private static string NewClusterId() => $"c{JobMasterRandomUtil.NewGuid4():N}";

    private static JobMasterClusterConnectionConfig CreateClusterConfig(string clusterId)
        => JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: true);
}
