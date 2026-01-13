using FluentAssertions;
using Moq;
using System;
using System.Linq;
using System.Threading.Tasks;
using JobMaster.Contracts.Models;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Models.GenericRecords;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Repositories.Master;
using JobMaster.Sdk.Services.Master;

namespace JobMaster.UnitTests.Services.Master;

public class JobMasterLoggerTests
{
    [Fact]
    public async Task Log_WhenMaxBatchSizeReached_ShouldFlushWithBulkInsertAsync()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var tcs = new TaskCompletionSource<IList<GenericRecordEntry>>(TaskCreationOptions.RunContinuationsAsynchronously);

        var repo = new Mock<IMasterGenericRecordRepository>(MockBehavior.Strict);
        repo.Setup(x => x.BulkInsertAsync(It.IsAny<IList<GenericRecordEntry>>()))
            .Returns<IList<GenericRecordEntry>>(records =>
            {
                tcs.TrySetResult(records);
                return Task.CompletedTask;
            });

        using var sut = new JobMasterLogger(clusterConfig, repo.Object);

        for (var i = 0; i < 100; i++)
        {
            sut.Log(JobMasterLogLevel.Info, $"m{i}", JobMasterLogSubjectType.Job, "s");
        }

        var completed = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(2)));
        completed.Should().Be(tcs.Task);

        var flushed = await tcs.Task;
        flushed.Should().NotBeNull();
        flushed.Count.Should().BeGreaterThan(0);
        flushed.Count.Should().BeLessThanOrEqualTo(100);

        repo.Verify(x => x.BulkInsertAsync(It.IsAny<IList<GenericRecordEntry>>()), Times.AtLeastOnce);
    }

    [Fact]
    public async Task QueryAsync_WhenCriteriaProvided_ShouldTranslateToRepoCriteria_AndConvertSubjectFields()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        GenericRecordQueryCriteria? captured = null;

        var repo = new Mock<IMasterGenericRecordRepository>(MockBehavior.Strict);
        repo
            .Setup(x => x.QueryAsync(MasterGenericRecordGroupIds.Log, It.IsAny<GenericRecordQueryCriteria>()))
            .Callback<string, GenericRecordQueryCriteria?>((_, crit) => captured = crit)
            .ReturnsAsync(() =>
            {
                var ts = new DateTime(2025, 1, 2, 3, 4, 5, DateTimeKind.Utc);
                var payload = new LogPayload()
                {
                    Level = (int)JobMasterLogLevel.Error,
                    Message = "hello",
                    TimestampUtc = ts,
                    Host = "h",
                    SourceMember = "DequeueSavePendingRecur",
                    SourceFile = "AgentJobsDispatcherRepository.cs",
                    SourceLine = 93
                };

                return (IList<GenericRecordEntry>)new[]
                {
                    GenericRecordEntry.Create(
                        clusterId,
                        MasterGenericRecordGroupIds.Log,
                        Guid.NewGuid(),
                        subjectType: nameof(JobMasterLogSubjectType.Job),
                        subjectId: "sid",
                        obj: payload)
                };
            });

        using var sut = new JobMasterLogger(clusterConfig, repo.Object);

        var from = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var to = new DateTime(2025, 1, 3, 0, 0, 0, DateTimeKind.Utc);

        var result = await sut.QueryAsync(new LogItemQueryCriteria
        {
            FromTimestamp = from,
            ToTimestamp = to,
            Level = JobMasterLogLevel.Error,
            Keyword = "hello",
            SubjectType = JobMasterLogSubjectType.Job,
            SubjectId = "sid"
        });

        captured.Should().NotBeNull();
        captured!.SubjectType.Should().Be(JobMasterLogSubjectType.Job.ToString());
        captured.SubjectIds.Should().ContainSingle().Which.Should().Be("sid");

        captured.Filters.Should().Contain(f => f.Key == "TimestampUtc" && f.Operation == GenericFilterOperation.Gte && Equals(f.Value, from));
        captured.Filters.Should().Contain(f => f.Key == "TimestampUtc" && f.Operation == GenericFilterOperation.Lte && Equals(f.Value, to));
        captured.Filters.Should().Contain(f => f.Key == "Level" && f.Operation == GenericFilterOperation.Eq && Equals(f.Value, JobMasterLogLevel.Error));
        captured.Filters.Should().Contain(f => f.Key == "Message" && f.Operation == GenericFilterOperation.Contains && Equals(f.Value, "hello"));

        result.Should().HaveCount(1);
        result[0].SubjectId.Should().Be("sid");
        result[0].SubjectType.Should().Be(JobMasterLogSubjectType.Job);
        result[0].Level.Should().Be(JobMasterLogLevel.Error);
        result[0].Message.Should().Be("hello");
        result[0].SourceMember.Should().Be("DequeueSavePendingRecur");
        result[0].SourceFile.Should().Be("AgentJobsDispatcherRepository.cs");
        result[0].SourceLine.Should().Be(93);

        repo.Verify(x => x.QueryAsync(MasterGenericRecordGroupIds.Log, It.IsAny<GenericRecordQueryCriteria>()), Times.Once);
    }

    [Fact]
    public async Task QueryAsync_WhenRepoEntryHasInvalidSubjectType_ShouldReturnNullSubjectType()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var repo = new Mock<IMasterGenericRecordRepository>(MockBehavior.Strict);
        repo
            .Setup(x => x.QueryAsync(MasterGenericRecordGroupIds.Log, It.IsAny<GenericRecordQueryCriteria>()))
            .ReturnsAsync(() =>
            {
                var ts = new DateTime(2025, 1, 2, 3, 4, 5, DateTimeKind.Utc);
                var payload = new
                {
                    Level = (int)JobMasterLogLevel.Info,
                    Message = "m",
                    TimestampUtc = ts,
                    Host = "h"
                };

                return (IList<GenericRecordEntry>)new[]
                {
                    GenericRecordEntry.Create(
                        clusterId,
                        MasterGenericRecordGroupIds.Log,
                        Guid.NewGuid(),
                        subjectType: "NotAType",
                        subjectId: "sid",
                        obj: payload)
                };
            });

        using var sut = new JobMasterLogger(clusterConfig, repo.Object);

        var result = await sut.QueryAsync(new LogItemQueryCriteria());

        result.Should().HaveCount(1);
        result[0].SubjectId.Should().Be("sid");
        result[0].SubjectType.Should().BeNull();
    }

    private static string NewClusterId() => $"c{Guid.NewGuid():N}";

    private static JobMasterClusterConnectionConfig CreateClusterConfig(string clusterId)
        => JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: true);
}
