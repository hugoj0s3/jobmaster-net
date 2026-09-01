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
    private readonly RavenDbRepositoryFixture fixture;

    public RavenDbLogsRepositoryConformanceTests(RavenDbRepositoryFixture fixture)
    {
        this.fixture = fixture;
    }

    // Production QueryAsync/CountAsync deliberately don't wait for index freshness (matches SQL's actual
    // consistency model -- read-after-write isn't guaranteed any sooner than that), so a test that writes
    // then immediately asserts needs to let indexing settle first, rather than assume the very next read
    // already reflects it. Same name/purpose as RepositoryJobsConformanceTests.SettleAsync -- not shared
    // with it since this class has no shared-with-SQL base to hang a virtual override on (see the class
    // doc above), but kept as its own method rather than an inline delay so future settle points here
    // stay consistent and easy to find.
    private static Task SettleAsync() => Task.Delay(TimeSpan.FromSeconds(2));

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
        await SettleAsync();

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
        await SettleAsync();

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
        await SettleAsync();

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
        await SettleAsync();

        var deleted = await fixture.MasterLogs.DeleteByTimestampAsync(cutoff, limit: 2);
        Assert.Equal(2, deleted);

        await SettleAsync();
        var remaining = await fixture.MasterLogs.QueryAsync(new LogItemQueryCriteria { ReferenceId = groupReferenceId, CountLimit = 100 });
        Assert.Equal(3, remaining.Count); // 4 older - 2 deleted + 1 newer
        Assert.Contains(remaining, x => x.Id == newer.Id);
    }

    [Fact]
    public async Task DeleteByTimestampAsync_WhenExcludeCategorySet_ShouldSkipThatCategory_RegardlessOfAge()
    {
        var groupReferenceId = "del-exclude-" + JobMasterRandomUtil.NewGuid4().ToString("N");
        // AddHours(-5), not -2: DeleteByTimestampAsync_ShouldDelete_OnlyOlderThanCutoff_RespectingLimit
        // uses -2 and deliberately leaves 2 items undeleted past its own limit -- a shared window here
        // would let those leftovers get swept into this test's own (unscoped, matches production) delete,
        // inflating the returned count.
        var baseTime = DateTime.UtcNow.AddHours(-5);
        var cutoff = baseTime.AddMinutes(5);

        var oldJobExecutionLog = NewItem(JobMasterLogLevel.Error, JobMasterLogCategory.JobExecution, "old exec log", referenceId: groupReferenceId, timestamp: baseTime);
        var oldOtherLog = NewItem(JobMasterLogLevel.Info, JobMasterLogCategory.Job, "old other log", referenceId: groupReferenceId, timestamp: baseTime);

        await fixture.MasterLogs.BulkInsertAsync(new List<LogItem> { oldJobExecutionLog, oldOtherLog });
        await SettleAsync();

        var deleted = await fixture.MasterLogs.DeleteByTimestampAsync(cutoff, limit: 100, excludeCategory: JobMasterLogCategory.JobExecution);
        Assert.Equal(1, deleted);

        await SettleAsync();
        var remaining = await fixture.MasterLogs.QueryAsync(new LogItemQueryCriteria { ReferenceId = groupReferenceId, CountLimit = 100 });
        Assert.Equal(oldJobExecutionLog.Id, Assert.Single(remaining).Id);
    }

    [Fact]
    public async Task QueryForReferenceIdsAsync_ShouldReturnOnlyMatchingCategoryAndReferenceIds()
    {
        var ref1 = "qfr-" + JobMasterRandomUtil.NewGuid4().ToString("N");
        var ref2 = "qfr-" + JobMasterRandomUtil.NewGuid4().ToString("N");
        var ref3 = "qfr-" + JobMasterRandomUtil.NewGuid4().ToString("N");

        var log1 = NewItem(JobMasterLogLevel.Error, JobMasterLogCategory.JobExecution, "for ref1", referenceId: ref1);
        var log2 = NewItem(JobMasterLogLevel.Error, JobMasterLogCategory.JobExecution, "for ref2", referenceId: ref2);
        var wrongCategory = NewItem(JobMasterLogLevel.Error, JobMasterLogCategory.Job, "wrong category", referenceId: ref1);
        var unrequestedRef = NewItem(JobMasterLogLevel.Error, JobMasterLogCategory.JobExecution, "for ref3", referenceId: ref3);

        await fixture.MasterLogs.BulkInsertAsync(new List<LogItem> { log1, log2, wrongCategory, unrequestedRef });
        await SettleAsync();

        var results = await fixture.MasterLogs.QueryForReferenceIdsAsync(JobMasterLogCategory.JobExecution, new List<string> { ref1, ref2 });

        Assert.Equal(2, results.Count);
        Assert.Contains(results, x => x.Id == log1.Id);
        Assert.Contains(results, x => x.Id == log2.Id);
        Assert.DoesNotContain(results, x => x.Id == wrongCategory.Id);
        Assert.DoesNotContain(results, x => x.Id == unrequestedRef.Id);
    }

    [Fact]
    public async Task DeleteByIdsAsync_ShouldDeleteOnlyTheGivenIds()
    {
        var item1 = NewItem(JobMasterLogLevel.Info, JobMasterLogCategory.JobExecution, "del-by-id-1");
        var item2 = NewItem(JobMasterLogLevel.Info, JobMasterLogCategory.JobExecution, "del-by-id-2");
        var untouched = NewItem(JobMasterLogLevel.Info, JobMasterLogCategory.JobExecution, "del-by-id-untouched");

        await fixture.MasterLogs.BulkInsertAsync(new List<LogItem> { item1, item2, untouched });

        var deleted = await fixture.MasterLogs.DeleteByIdsAsync(new List<Guid> { item1.Id, item2.Id });
        Assert.Equal(2, deleted);

        Assert.Null(await fixture.MasterLogs.GetAsync(item1.Id));
        Assert.Null(await fixture.MasterLogs.GetAsync(item2.Id));
        Assert.NotNull(await fixture.MasterLogs.GetAsync(untouched.Id));
    }
}
