using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Utils;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.RavenDb;

// No shared "Logs" RepoConformance category exists for any provider (SQL included), so this is a
// RavenDB-only test class rather than a subclass of a reused abstract base -- see the class doc on
// RavenDbRepositoryFixture.MasterLogs for why creating a shared base now (with a single consumer) isn't
// well justified.
[Collection("RavenDbRepoConformance")]
[Trait("DB", "RavenDb")]
public sealed class RavenDbLogsRepositoryConformanceTests
{
    // Production QueryAsync/CountAsync deliberately don't wait for index freshness (matches SQL's actual
    // consistency model -- see the RavenDB provider plan's design reference), so a test that writes then
    // immediately asserts needs to let indexing settle first, rather than assume the very next read
    // already reflects it.
    private static readonly TimeSpan IndexSettleDelay = TimeSpan.FromSeconds(2);

    private readonly RavenDbRepositoryFixture fixture;

    public RavenDbLogsRepositoryConformanceTests(RavenDbRepositoryFixture fixture)
    {
        this.fixture = fixture;
    }

    private LogItem NewItem(JobMasterLogLevel level, JobMasterLogCategory? category, string message, string? referenceId = null, DateTime? timestamp = null)
    {
        return new LogItem
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            ClusterId = fixture.ClusterId,
            Level = level,
            Message = message,
            Category = category,
            ReferenceId = referenceId,
            TimestampUtc = timestamp ?? DateTime.UtcNow,
            Host = "test-host",
            SourceMember = "TestMember",
            SourceFile = "Test.cs",
            SourceLine = 42,
        };
    }

    [Fact]
    public async Task BulkInsertAndGet_ShouldRoundTrip_AllFields()
    {
        var item = NewItem(JobMasterLogLevel.Warning, JobMasterLogCategory.Cluster, "hello world", referenceId: "ref-1");

        await fixture.MasterLogs.BulkInsertAsync(new List<LogItem> { item });

        // GetAsync is a point lookup by id -- reads the document store directly, not an index, so no
        // settle delay needed here (unlike the Query/Count-based tests below).
        var fromDb = await fixture.MasterLogs.GetAsync(item.Id);
        Assert.NotNull(fromDb);
        Assert.Equal(item.Id, fromDb!.Id);
        Assert.Equal(item.ClusterId, fromDb.ClusterId);
        Assert.Equal(item.Level, fromDb.Level);
        Assert.Equal(item.Message, fromDb.Message);
        Assert.Equal(item.Category, fromDb.Category);
        Assert.Equal(item.ReferenceId, fromDb.ReferenceId);
        Assert.Equal(item.Host, fromDb.Host);
        Assert.Equal(item.SourceMember, fromDb.SourceMember);
        Assert.Equal(item.SourceFile, fromDb.SourceFile);
        Assert.Equal(item.SourceLine, fromDb.SourceLine);
        Assert.True((item.TimestampUtc - fromDb.TimestampUtc).Duration() < TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task GetAsync_ShouldReturnNull_WhenMissing()
    {
        var result = await fixture.MasterLogs.GetAsync(Guid.NewGuid());
        Assert.Null(result);
    }

    [Fact]
    public async Task QueryAsync_ShouldFilter_ByLevelCategoryReferenceIdAndKeyword()
    {
        var refId = "ref-" + JobMasterRandomUtil.NewGuid4().ToString("N");

        var warn = NewItem(JobMasterLogLevel.Warning, JobMasterLogCategory.Bucket, "bucket rebalanced", referenceId: refId);
        var error = NewItem(JobMasterLogLevel.Error, JobMasterLogCategory.Job, "job failed unexpectedly", referenceId: refId);
        var info = NewItem(JobMasterLogLevel.Info, JobMasterLogCategory.Bucket, "unrelated info message");

        await fixture.MasterLogs.BulkInsertAsync(new List<LogItem> { warn, error, info });
        await Task.Delay(IndexSettleDelay);

        var byLevel = await fixture.MasterLogs.QueryAsync(new LogItemQueryCriteria { Level = JobMasterLogLevel.Warning, CountLimit = 100 });
        Assert.Contains(byLevel, x => x.Id == warn.Id);
        Assert.DoesNotContain(byLevel, x => x.Id == error.Id);

        var byCategory = await fixture.MasterLogs.QueryAsync(new LogItemQueryCriteria { Category = JobMasterLogCategory.Bucket, CountLimit = 100 });
        Assert.Contains(byCategory, x => x.Id == warn.Id);
        Assert.Contains(byCategory, x => x.Id == info.Id);
        Assert.DoesNotContain(byCategory, x => x.Id == error.Id);

        var byReferenceId = await fixture.MasterLogs.QueryAsync(new LogItemQueryCriteria { ReferenceId = refId, CountLimit = 100 });
        Assert.Contains(byReferenceId, x => x.Id == warn.Id);
        Assert.Contains(byReferenceId, x => x.Id == error.Id);
        Assert.DoesNotContain(byReferenceId, x => x.Id == info.Id);

        var byKeyword = await fixture.MasterLogs.QueryAsync(new LogItemQueryCriteria { Keyword = "rebalanced", CountLimit = 100 });
        Assert.Contains(byKeyword, x => x.Id == warn.Id);
        Assert.DoesNotContain(byKeyword, x => x.Id == error.Id);
        Assert.DoesNotContain(byKeyword, x => x.Id == info.Id);
    }

    [Fact]
    public async Task QueryAsync_ShouldFilter_ByTimestampRange_AndRespectPaging()
    {
        var groupReferenceId = "range-" + JobMasterRandomUtil.NewGuid4().ToString("N");
        var baseTime = DateTime.UtcNow.AddHours(-1);

        var items = new List<LogItem>();
        for (var i = 0; i < 5; i++)
        {
            items.Add(NewItem(JobMasterLogLevel.Info, JobMasterLogCategory.Api, $"msg-{i}", referenceId: groupReferenceId, timestamp: baseTime.AddMinutes(i)));
        }
        await fixture.MasterLogs.BulkInsertAsync(items);
        await Task.Delay(IndexSettleDelay);

        var inRange = await fixture.MasterLogs.QueryAsync(new LogItemQueryCriteria
        {
            ReferenceId = groupReferenceId,
            FromTimestamp = baseTime.AddMinutes(1).AddSeconds(-1),
            ToTimestamp = baseTime.AddMinutes(3).AddSeconds(1),
            CountLimit = 100,
        });
        Assert.Equal(3, inRange.Count); // i = 1,2,3

        var paged = await fixture.MasterLogs.QueryAsync(new LogItemQueryCriteria
        {
            ReferenceId = groupReferenceId,
            CountLimit = 2,
            Offset = 1,
        });
        Assert.Equal(2, paged.Count);
    }

    [Fact]
    public async Task CountAsync_ShouldMatch_QueryAsync_TotalRegardlessOfLimit()
    {
        var groupReferenceId = "count-" + JobMasterRandomUtil.NewGuid4().ToString("N");
        var items = Enumerable.Range(0, 7)
            .Select(i => NewItem(JobMasterLogLevel.Debug, JobMasterLogCategory.RecurringSchedule, $"count-msg-{i}", referenceId: groupReferenceId))
            .ToList();

        await fixture.MasterLogs.BulkInsertAsync(items);
        await Task.Delay(IndexSettleDelay);

        var count = await fixture.MasterLogs.CountAsync(new LogItemQueryCriteria { ReferenceId = groupReferenceId, CountLimit = 2 });
        Assert.Equal(7, count);
    }

    [Fact]
    public async Task DeleteByTimestampAsync_ShouldDelete_OnlyOlderThanCutoff_RespectingLimit()
    {
        var groupReferenceId = "del-" + JobMasterRandomUtil.NewGuid4().ToString("N");
        var baseTime = DateTime.UtcNow.AddHours(-2);
        var cutoff = baseTime.AddMinutes(5);

        var older = Enumerable.Range(0, 4)
            .Select(i => NewItem(JobMasterLogLevel.Info, null, $"old-{i}", referenceId: groupReferenceId, timestamp: baseTime.AddMinutes(i)))
            .ToList();
        var newer = NewItem(JobMasterLogLevel.Info, null, "new", referenceId: groupReferenceId, timestamp: baseTime.AddMinutes(30));

        await fixture.MasterLogs.BulkInsertAsync(older);
        await fixture.MasterLogs.BulkInsertAsync(new List<LogItem> { newer });
        // Let the writes settle before the one-shot delete -- DeleteByTimestampAsync uses AllowStale, and
        // unlike a read it isn't safe to retry (a retry would delete additional records beyond `limit`,
        // not just re-check the same outcome), so the writes need to be visible before calling it once.
        await Task.Delay(IndexSettleDelay);

        var deleted = await fixture.MasterLogs.DeleteByTimestampAsync(cutoff, limit: 2);
        Assert.Equal(2, deleted);

        await Task.Delay(IndexSettleDelay);
        var remaining = await fixture.MasterLogs.QueryAsync(new LogItemQueryCriteria { ReferenceId = groupReferenceId, CountLimit = 100 });
        Assert.Equal(3, remaining.Count); // 4 older - 2 deleted + 1 newer
        Assert.Contains(remaining, x => x.Id == newer.Id);
    }
}
