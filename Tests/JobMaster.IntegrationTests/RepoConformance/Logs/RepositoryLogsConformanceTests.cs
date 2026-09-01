using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Utils;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.Logs;

// Some providers' Query/CountAsync/DeleteByTimestampAsync/QueryForReferenceIdsAsync don't wait for read
// freshness after a write (RavenDB deliberately doesn't -- see RavenDbMasterLogsRepository's own
// comments), so a write immediately followed by one of those reads in the same test needs to
// accommodate that lag itself rather than assume it. SettleAsync is called after seeding, before the
// read that depends on seeing it -- point lookups (GetAsync) never need it. No-op by default (SQL
// providers have no equivalent lag); overridden per provider that actually needs it, same pattern as
// RepositoryJobsConformanceTests.SettleAsync.
public abstract class RepositoryLogsConformanceTests<TFixture>
    where TFixture : RepositoryFixtureBase
{
    protected TFixture Fixture { get; }

    protected RepositoryLogsConformanceTests(TFixture fixture)
    {
        Fixture = fixture;
    }

    protected virtual Task SettleAsync() => Task.CompletedTask;

    private LogItem NewItem(JobMasterLogLevel level, JobMasterLogCategory? category, string message, string? referenceId = null, DateTime? timestamp = null)
    {
        return new LogItem
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            ClusterId = Fixture.ClusterId,
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

        await Fixture.MasterLogs.BulkInsertAsync(new List<LogItem> { item });

        // GetAsync is a point lookup by id -- reads the document store directly, not an index, so no
        // settle delay needed here (unlike the Query/Count-based tests below).
        var fromDb = await Fixture.MasterLogs.GetAsync(item.Id);
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
        var result = await Fixture.MasterLogs.GetAsync(Guid.NewGuid());
        Assert.Null(result);
    }

    [Fact]
    public async Task QueryAsync_ShouldFilter_ByLevelCategoryReferenceIdAndKeyword()
    {
        var refId = "ref-" + JobMasterRandomUtil.NewGuid4().ToString("N");

        var warn = NewItem(JobMasterLogLevel.Warning, JobMasterLogCategory.Bucket, "bucket rebalanced", referenceId: refId);
        var error = NewItem(JobMasterLogLevel.Error, JobMasterLogCategory.Job, "job failed unexpectedly", referenceId: refId);
        var info = NewItem(JobMasterLogLevel.Info, JobMasterLogCategory.Bucket, "unrelated info message");

        await Fixture.MasterLogs.BulkInsertAsync(new List<LogItem> { warn, error, info });
        await SettleAsync();

        var byLevel = await Fixture.MasterLogs.QueryAsync(new LogItemQueryCriteria { Level = JobMasterLogLevel.Warning, CountLimit = 100 });
        Assert.Contains(byLevel, x => x.Id == warn.Id);
        Assert.DoesNotContain(byLevel, x => x.Id == error.Id);

        var byCategory = await Fixture.MasterLogs.QueryAsync(new LogItemQueryCriteria { Category = JobMasterLogCategory.Bucket, CountLimit = 100 });
        Assert.Contains(byCategory, x => x.Id == warn.Id);
        Assert.Contains(byCategory, x => x.Id == info.Id);
        Assert.DoesNotContain(byCategory, x => x.Id == error.Id);

        var byReferenceId = await Fixture.MasterLogs.QueryAsync(new LogItemQueryCriteria { ReferenceId = refId, CountLimit = 100 });
        Assert.Contains(byReferenceId, x => x.Id == warn.Id);
        Assert.Contains(byReferenceId, x => x.Id == error.Id);
        Assert.DoesNotContain(byReferenceId, x => x.Id == info.Id);

        var byKeyword = await Fixture.MasterLogs.QueryAsync(new LogItemQueryCriteria { Keyword = "rebalanced", CountLimit = 100 });
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
        await Fixture.MasterLogs.BulkInsertAsync(items);
        await SettleAsync();

        var inRange = await Fixture.MasterLogs.QueryAsync(new LogItemQueryCriteria
        {
            ReferenceId = groupReferenceId,
            FromTimestamp = baseTime.AddMinutes(1).AddSeconds(-1),
            ToTimestamp = baseTime.AddMinutes(3).AddSeconds(1),
            CountLimit = 100,
        });
        Assert.Equal(3, inRange.Count); // i = 1,2,3

        var paged = await Fixture.MasterLogs.QueryAsync(new LogItemQueryCriteria
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

        await Fixture.MasterLogs.BulkInsertAsync(items);
        await SettleAsync();

        var count = await Fixture.MasterLogs.CountAsync(new LogItemQueryCriteria { ReferenceId = groupReferenceId, CountLimit = 2 });
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

        await Fixture.MasterLogs.BulkInsertAsync(older);
        await Fixture.MasterLogs.BulkInsertAsync(new List<LogItem> { newer });
        // Let the writes settle before the one-shot delete -- DeleteByTimestampAsync isn't safe to retry
        // (a retry would delete additional records beyond `limit`, not just re-check the same outcome),
        // so the writes need to be visible before calling it once.
        await SettleAsync();

        var deleted = await Fixture.MasterLogs.DeleteByTimestampAsync(cutoff, limit: 2);
        Assert.Equal(2, deleted);

        await SettleAsync();
        var remaining = await Fixture.MasterLogs.QueryAsync(new LogItemQueryCriteria { ReferenceId = groupReferenceId, CountLimit = 100 });
        Assert.Equal(3, remaining.Count); // 4 older - 2 deleted + 1 newer
        Assert.Contains(remaining, x => x.Id == newer.Id);

        // DeleteByTimestampAsync is an unscoped, cluster-wide blanket delete with no lower bound -- any
        // row left behind here would be reachable by ANY other test's own blanket delete call too
        // (e.g. DeleteByTimestampAsync_WhenExcludeCategorySet_ShouldSkipThatCategory_RegardlessOfAge),
        // regardless of how far apart their timestamp windows are chosen, since xUnit doesn't guarantee
        // method execution order within a class. Clean up everything this test created so nothing
        // persists for a sibling test to interact with.
        await Fixture.MasterLogs.DeleteByIdsAsync(remaining.Select(x => x.Id).ToList());
    }

    [Fact]
    public async Task DeleteByTimestampAsync_WhenExcludeCategorySet_ShouldSkipThatCategory_RegardlessOfAge()
    {
        var groupReferenceId = "del-exclude-" + JobMasterRandomUtil.NewGuid4().ToString("N");
        var baseTime = DateTime.UtcNow.AddHours(-2);
        var cutoff = baseTime.AddMinutes(5);

        var oldJobExecutionLog = NewItem(JobMasterLogLevel.Error, JobMasterLogCategory.JobExecution, "old exec log", referenceId: groupReferenceId, timestamp: baseTime);
        var oldOtherLog = NewItem(JobMasterLogLevel.Info, JobMasterLogCategory.Job, "old other log", referenceId: groupReferenceId, timestamp: baseTime);

        await Fixture.MasterLogs.BulkInsertAsync(new List<LogItem> { oldJobExecutionLog, oldOtherLog });
        await SettleAsync();

        var deleted = await Fixture.MasterLogs.DeleteByTimestampAsync(cutoff, limit: 100, excludeCategory: JobMasterLogCategory.JobExecution);
        Assert.Equal(1, deleted);

        await SettleAsync();
        var remaining = await Fixture.MasterLogs.QueryAsync(new LogItemQueryCriteria { ReferenceId = groupReferenceId, CountLimit = 100 });
        Assert.Equal(oldJobExecutionLog.Id, Assert.Single(remaining).Id);

        // DeleteByTimestampAsync is an unscoped, cluster-wide blanket delete with no lower bound -- the
        // excluded log here is deliberately immortal to THIS test's own delete call, but would still be
        // reachable by any OTHER test's own blanket delete (e.g.
        // DeleteByTimestampAsync_ShouldDelete_OnlyOlderThanCutoff_RespectingLimit), regardless of how far
        // apart their timestamp windows are chosen, since xUnit doesn't guarantee method execution order
        // within a class. This bit for real against Postgres/SqlServer the first time this suite ran
        // against real SQL providers ("Expected 3, Actual 4" in that sibling test). Clean it up explicitly
        // via DeleteByIdsAsync (not category-aware) so nothing persists for a sibling test to interact with.
        await Fixture.MasterLogs.DeleteByIdsAsync(new List<Guid> { oldJobExecutionLog.Id });
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

        await Fixture.MasterLogs.BulkInsertAsync(new List<LogItem> { log1, log2, wrongCategory, unrequestedRef });
        await SettleAsync();

        var results = await Fixture.MasterLogs.QueryForReferenceIdsAsync(JobMasterLogCategory.JobExecution, new List<string> { ref1, ref2 });

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

        await Fixture.MasterLogs.BulkInsertAsync(new List<LogItem> { item1, item2, untouched });

        var deleted = await Fixture.MasterLogs.DeleteByIdsAsync(new List<Guid> { item1.Id, item2.Id });
        Assert.Equal(2, deleted);

        Assert.Null(await Fixture.MasterLogs.GetAsync(item1.Id));
        Assert.Null(await Fixture.MasterLogs.GetAsync(item2.Id));
        Assert.NotNull(await Fixture.MasterLogs.GetAsync(untouched.Id));
    }
}
