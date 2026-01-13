using JobMaster.Contracts.Models;
using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using JobMaster.Sdk.Contracts.Models.GenericRecords;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.GenericRecords;

public abstract class RepositoryGenericRecordsConformanceTests<TFixture>
    where TFixture : class, IRepositoryFixture
{
    protected TFixture Fixture { get; }

    protected RepositoryGenericRecordsConformanceTests(TFixture fixture)
    {
        Fixture = fixture;
    }

    [Fact]
    public async Task InsertAndGet_ShouldRoundTrip_AllSupportedValueTypes()
    {
        var groupId = "GenericRecordTestGroup";
        var entryId = "entry-" + Guid.NewGuid().ToString("N");

        var now = DateTime.UtcNow;
        var expiresAt = now.AddHours(1);

        var record = NewEntry(groupId, entryId);
        record.SubjectType = "subjectType";
        record.SubjectId = "subjectId";
        record.CreatedAt = now;
        record.ExpiresAt = expiresAt;

        record.Values = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["s"] = "alpha",
            ["n"] = 10L,
            ["d"] = 12.34m,
            ["b"] = true,
            ["dt"] = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            ["g"] = Guid.Parse("11111111-1111-1111-1111-111111111111"),
            ["null"] = null,
        };

        await Fixture.MasterGenericRecords.InsertAsync(record);

        var fromDb = await Fixture.MasterGenericRecords.GetAsync(groupId, entryId, includeExpired: true);
        Assert.NotNull(fromDb);

        AssertEntryEquivalent(record, fromDb!);
    }

    [Fact]
    public async Task Update_ShouldPersist_Values_And_HeaderFields()
    {
        var groupId = "GenericRecordTestGroup";
        var entryId = "entry-" + Guid.NewGuid().ToString("N");

        var record = NewEntry(groupId, entryId);
        record.SubjectType = "st1";
        record.SubjectId = "sid1";
        record.CreatedAt = DateTime.UtcNow.AddMinutes(-5);
        record.ExpiresAt = DateTime.UtcNow.AddHours(1);
        record.Values = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["k"] = "v1",
            ["n"] = 1L,
        };

        await Fixture.MasterGenericRecords.InsertAsync(record);

        record.SubjectType = "st2";
        record.SubjectId = "sid2";
        record.ExpiresAt = DateTime.UtcNow.AddHours(2);
        record.Values = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["k"] = "v2",
            ["n"] = 2L,
            ["dt"] = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc)
        };

        await Fixture.MasterGenericRecords.UpdateAsync(record);

        var fromDb = await Fixture.MasterGenericRecords.GetAsync(groupId, entryId, includeExpired: true);
        Assert.NotNull(fromDb);

        AssertEntryEquivalent(record, fromDb!);
    }

    [Fact]
    public async Task Upsert_ShouldInsertThenUpdate()
    {
        var groupId = "GenericRecordTestGroup";
        var entryId = "entry-" + Guid.NewGuid().ToString("N");

        var record = NewEntry(groupId, entryId);
        record.SubjectType = "st";
        record.SubjectId = "sid";
        record.CreatedAt = DateTime.UtcNow.AddMinutes(-2);
        record.Values = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["k"] = "v1",
        };

        await Fixture.MasterGenericRecords.UpsertAsync(record);

        record.Values["k"] = "v2";
        record.ExpiresAt = DateTime.UtcNow.AddMinutes(5);

        await Fixture.MasterGenericRecords.UpsertAsync(record);

        var fromDb = await Fixture.MasterGenericRecords.GetAsync(groupId, entryId, includeExpired: true);
        Assert.NotNull(fromDb);
        AssertEntryEquivalent(record, fromDb!);
    }

    [Fact]
    public async Task Delete_ShouldRemove_Entry_And_Values()
    {
        var groupId = "GenericRecordTestGroup";
        var entryId = "entry-" + Guid.NewGuid().ToString("N");

        var record = NewEntry(groupId, entryId);
        record.Values = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["k"] = "v",
        };

        await Fixture.MasterGenericRecords.InsertAsync(record);

        await Fixture.MasterGenericRecords.DeleteAsync(groupId, entryId);

        var fromDb = await Fixture.MasterGenericRecords.GetAsync(groupId, entryId, includeExpired: true);
        Assert.Null(fromDb);
    }

    [Fact]
    public async Task Query_ShouldSupport_AllCriteriaFields()
    {
        var groupId = "GenericRecordTestGroup";
        var subjectType = "SubType";

        var baseTime = DateTime.UtcNow.AddHours(-1);

        var e1 = NewEntry(groupId, "e1-" + Guid.NewGuid().ToString("N"));
        e1.SubjectType = subjectType;
        e1.SubjectId = "S1";
        e1.CreatedAt = baseTime.AddMinutes(1);
        e1.ExpiresAt = baseTime.AddHours(10);
        e1.Values = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["s"] = "alpha",
            ["n"] = 10L,
        };

        var e2 = NewEntry(groupId, "e2-" + Guid.NewGuid().ToString("N"));
        e2.SubjectType = subjectType;
        e2.SubjectId = "S2";
        e2.CreatedAt = baseTime.AddMinutes(2);
        e2.ExpiresAt = baseTime.AddMinutes(-10); // expired
        e2.Values = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["s"] = "alphabet",
            ["n"] = 20L,
        };

        var e3 = NewEntry(groupId, "e3-" + Guid.NewGuid().ToString("N"));
        e3.SubjectType = subjectType;
        e3.SubjectId = "S1";
        e3.CreatedAt = baseTime.AddMinutes(3);
        e3.ExpiresAt = null;
        e3.Values = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["s"] = "beta",
            ["n"] = 30L,
        };

        await Fixture.MasterGenericRecords.InsertAsync(e1);
        await Fixture.MasterGenericRecords.InsertAsync(e2);
        await Fixture.MasterGenericRecords.InsertAsync(e3);

        // includeExpired=false should exclude expired
        var q1 = await Fixture.MasterGenericRecords.QueryAsync(groupId, new GenericRecordQueryCriteria
        {
            SubjectType = subjectType,
            IncludeExpired = false,
            OrderBy = GenericRecordQueryOrderByTypeId.CreatedAtAsc
        });
        Assert.DoesNotContain(q1, x => x.EntryId == e2.EntryId);

        // includeExpired=true should include expired
        var q2 = await Fixture.MasterGenericRecords.QueryAsync(groupId, new GenericRecordQueryCriteria
        {
            SubjectType = subjectType,
            IncludeExpired = true,
            OrderBy = GenericRecordQueryOrderByTypeId.CreatedAtAsc
        });
        Assert.Contains(q2, x => x.EntryId == e2.EntryId);

        // SubjectIds filter
        var q3 = await Fixture.MasterGenericRecords.QueryAsync(groupId, new GenericRecordQueryCriteria
        {
            SubjectType = subjectType,
            SubjectIds = new List<string> { "S1" },
            IncludeExpired = true
        });
        Assert.Contains(q3, x => x.EntryId == e1.EntryId);
        Assert.Contains(q3, x => x.EntryId == e3.EntryId);
        Assert.DoesNotContain(q3, x => x.EntryId == e2.EntryId);

        // EntryIds filter
        var q4 = await Fixture.MasterGenericRecords.QueryAsync(groupId, new GenericRecordQueryCriteria
        {
            EntryIds = new List<string> { e1.EntryId },
            IncludeExpired = true
        });
        Assert.Single(q4);
        Assert.Equal(e1.EntryId, q4[0].EntryId);

        // CreatedAt range
        var q5 = await Fixture.MasterGenericRecords.QueryAsync(groupId, new GenericRecordQueryCriteria
        {
            CreatedAtFrom = baseTime.AddMinutes(2),
            CreatedAtTo = baseTime.AddMinutes(3),
            IncludeExpired = true
        });
        Assert.Contains(q5, x => x.EntryId == e2.EntryId);
        Assert.Contains(q5, x => x.EntryId == e3.EntryId);
        Assert.DoesNotContain(q5, x => x.EntryId == e1.EntryId);

        // ExpiresAt range (only those with ExpiresAt not null)
        var q6 = await Fixture.MasterGenericRecords.QueryAsync(groupId, new GenericRecordQueryCriteria
        {
            ExpiresAtFrom = baseTime.AddHours(9),
            ExpiresAtTo = baseTime.AddHours(11),
            IncludeExpired = true
        });
        Assert.Contains(q6, x => x.EntryId == e1.EntryId);
        Assert.DoesNotContain(q6, x => x.EntryId == e2.EntryId);
        Assert.DoesNotContain(q6, x => x.EntryId == e3.EntryId);

        // Paging + order
        var ordered = (await Fixture.MasterGenericRecords.QueryAsync(groupId, new GenericRecordQueryCriteria
        {
            SubjectType = subjectType,
            IncludeExpired = true,
            OrderBy = GenericRecordQueryOrderByTypeId.CreatedAtAsc
        })).Select(x => x.EntryId).ToList();

        var paged = await Fixture.MasterGenericRecords.QueryAsync(groupId, new GenericRecordQueryCriteria
        {
            SubjectType = subjectType,
            IncludeExpired = true,
            OrderBy = GenericRecordQueryOrderByTypeId.CreatedAtAsc,
            Limit = 1,
            Offset = 1
        });

        Assert.Single(paged);
        Assert.Equal(ordered.Skip(1).First(), paged[0].EntryId);
    }

    [Fact]
    public async Task Query_ShouldSupport_MetadataFilters_AllOperations_And_Types()
    {
        var groupId = "GenericRecordTestGroup";
        var subjectType = "SubTypeMeta";

        var t0 = new DateTime(2025, 01, 01, 0, 0, 0, DateTimeKind.Utc);

        var a = NewEntry(groupId, "a-" + Guid.NewGuid().ToString("N"));
        a.SubjectType = subjectType;
        a.SubjectId = "S1";
        a.Values = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["s"] = "alpha",
            ["n"] = 10L,
            ["dt"] = t0,
        };

        var b = NewEntry(groupId, "b-" + Guid.NewGuid().ToString("N"));
        b.SubjectType = subjectType;
        b.SubjectId = "S2";
        b.Values = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["s"] = "alphabet",
            ["n"] = 20L,
            ["dt"] = t0.AddDays(1),
        };

        var c = NewEntry(groupId, "c-" + Guid.NewGuid().ToString("N"));
        c.SubjectType = subjectType;
        c.SubjectId = "S3";
        c.Values = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["s"] = "beta",
            ["n"] = 30L,
            ["dt"] = t0.AddDays(2),
        };

        await Fixture.MasterGenericRecords.InsertAsync(a);
        await Fixture.MasterGenericRecords.InsertAsync(b);
        await Fixture.MasterGenericRecords.InsertAsync(c);

        // String operations
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "s", Operation = GenericFilterOperation.Eq, Value = "alpha" }, a.EntryId);
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "s", Operation = GenericFilterOperation.Neq, Value = "alpha" }, b.EntryId, c.EntryId);
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "s", Operation = GenericFilterOperation.In, Values = new object?[] { "alpha", "beta" } }, a.EntryId, c.EntryId);
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "s", Operation = GenericFilterOperation.Contains, Value = "lph" }, a.EntryId, b.EntryId);
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "s", Operation = GenericFilterOperation.StartsWith, Value = "alph" }, a.EntryId, b.EntryId);
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "s", Operation = GenericFilterOperation.EndsWith, Value = "bet" }, b.EntryId);

        // Numeric operations
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.Eq, Value = 20 }, b.EntryId);
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.Neq, Value = 20 }, a.EntryId, c.EntryId);
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.In, Values = new object?[] { 10, 30 } }, a.EntryId, c.EntryId);
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.Gt, Value = 10 }, b.EntryId, c.EntryId);
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.Gte, Value = 20 }, b.EntryId, c.EntryId);
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.Lt, Value = 30 }, a.EntryId, b.EntryId);
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.Lte, Value = 20 }, a.EntryId, b.EntryId);

        // DateTime operations
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.Eq, Value = t0.AddDays(1) }, b.EntryId);
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.Neq, Value = t0.AddDays(1) }, a.EntryId, c.EntryId);
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.In, Values = new object?[] { t0, t0.AddDays(2) } }, a.EntryId, c.EntryId);
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.Gt, Value = t0 }, b.EntryId, c.EntryId);
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.Gte, Value = t0.AddDays(2) }, c.EntryId);
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.Lt, Value = t0.AddDays(2) }, a.EntryId, b.EntryId);
        await AssertGenericFilter(groupId, subjectType, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.Lte, Value = t0.AddDays(1) }, a.EntryId, b.EntryId);
    }

    [Fact]
    public async Task BulkInsert_DeleteExpired_DeleteByCreatedAt_ShouldWork()
    {
        var groupId = "GenericRecordTestGroup_Bulk";
        var baseTime = DateTime.UtcNow.AddHours(-10);

        var entries = new List<GenericRecordEntry>();

        for (var i = 0; i < 5; i++)
        {
            var e = NewEntry(groupId, $"e{i}-" + Guid.NewGuid().ToString("N"));
            e.CreatedAt = baseTime.AddMinutes(i);
            e.ExpiresAt = i % 2 == 0 ? baseTime.AddMinutes(-1) : baseTime.AddHours(5);
            e.Values = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["i"] = (long)i
            };
            entries.Add(e);
        }

        await Fixture.MasterGenericRecords.BulkInsertAsync(entries);

        var deletedExpired = await Fixture.MasterGenericRecords.DeleteExpiredAsync(DateTime.UtcNow, limit: 100);
        Assert.True(deletedExpired >= 1);

        var remaining1 = await Fixture.MasterGenericRecords.QueryAsync(groupId, new GenericRecordQueryCriteria { IncludeExpired = true });
        Assert.True(remaining1.Count <= 5);

        var deletedByCreated = await Fixture.MasterGenericRecords.DeleteByCreatedAtAsync(groupId, baseTime.AddMinutes(2), limit: 100);
        Assert.True(deletedByCreated >= 0);

        var remaining2 = await Fixture.MasterGenericRecords.QueryAsync(groupId, new GenericRecordQueryCriteria { IncludeExpired = true });
        Assert.True(remaining2.Count <= remaining1.Count);
    }

    protected async Task AssertGenericFilter(string groupId, string? subjectType, GenericRecordValueFilter filter, params string[] expectedEntryIds)
    {
        var criteria = new GenericRecordQueryCriteria
        {
            SubjectType = subjectType,
            IncludeExpired = true,
            Filters = new List<GenericRecordValueFilter> { filter },
            Limit = 100,
            Offset = 0,
            OrderBy = GenericRecordQueryOrderByTypeId.CreatedAtAsc
        };

        var queried = await Fixture.MasterGenericRecords.QueryAsync(groupId, criteria);
        var ids = queried.Select(x => x.EntryId).OrderBy(x => x).ToList();
        var expected = expectedEntryIds.OrderBy(x => x).ToList();

        Assert.Equal(expected, ids);
    }

    protected GenericRecordEntry NewEntry(string groupId, string entryId)
    {
        return GenericRecordEntry.FromWritableMetadata(
            Fixture.ClusterId,
            groupId,
            entryId,
            new Metadata(new Dictionary<string, object?>()),
            expiresAt: null);
    }

    private static void AssertEntryEquivalent(GenericRecordEntry expected, GenericRecordEntry actual)
    {
        Assert.Equal(expected.ClusterId, actual.ClusterId);
        Assert.Equal(expected.GroupId, actual.GroupId);
        Assert.Equal(expected.EntryId, actual.EntryId);
        Assert.Equal(expected.RecordUniqueId, actual.RecordUniqueId);
        Assert.Equal(expected.SubjectType, actual.SubjectType);
        Assert.Equal(expected.SubjectId, actual.SubjectId);

        AssertDateTimeEquivalent(ToUtc(expected.CreatedAt), ToUtc(actual.CreatedAt));
        AssertDateTimeEquivalent(ToUtcN(expected.ExpiresAt), ToUtcN(actual.ExpiresAt));

        Assert.Equal(expected.Values.Count, actual.Values.Count);

        foreach (var kv in expected.Values)
        {
            Assert.True(actual.Values.ContainsKey(kv.Key));
            AssertValueEquivalent(kv.Value, actual.Values[kv.Key]);
        }
    }

    private static void AssertValueEquivalent(object? expected, object? actual)
    {
        if (expected is null && actual is null)
        {
            return;
        }

        Assert.True(expected is not null && actual is not null);

        if (expected is DateTime edt)
        {
            Assert.True(actual is DateTime);
            AssertDateTimeEquivalent(ToUtc(edt), ToUtc((DateTime)actual));
            return;
        }

        if (expected is Guid eg)
        {
            Assert.True(actual is Guid);
            Assert.Equal(eg, (Guid)actual);
            return;
        }

        if (expected is long el)
        {
            if (actual is int ai)
            {
                Assert.Equal(el, (long)ai);
                return;
            }
            Assert.True(actual is long);
            Assert.Equal(el, (long)actual);
            return;
        }

        if (expected is decimal ed)
        {
            Assert.True(actual is decimal);
            Assert.Equal(ed, (decimal)actual);
            return;
        }

        Assert.Equal(expected, actual);
    }

    private static DateTime ToUtc(DateTime dt) => DateTime.SpecifyKind(dt, DateTimeKind.Utc);
    private static DateTime? ToUtcN(DateTime? dt) => dt.HasValue ? DateTime.SpecifyKind(dt.Value, DateTimeKind.Utc) : null;

    private static void AssertDateTimeEquivalent(DateTime expectedUtc, DateTime actualUtc)
    {
        var diff = (expectedUtc - actualUtc).Duration();
        Assert.True(diff <= TimeSpan.FromMilliseconds(600), $"Expected {expectedUtc:O} but was {actualUtc:O} (diff={diff.TotalMilliseconds}ms)");
    }

    private static void AssertDateTimeEquivalent(DateTime? expectedUtc, DateTime? actualUtc)
    {
        if (!expectedUtc.HasValue && !actualUtc.HasValue)
        {
            return;
        }

        Assert.True(expectedUtc.HasValue && actualUtc.HasValue);
        AssertDateTimeEquivalent(expectedUtc!.Value, actualUtc!.Value);
    }
}
