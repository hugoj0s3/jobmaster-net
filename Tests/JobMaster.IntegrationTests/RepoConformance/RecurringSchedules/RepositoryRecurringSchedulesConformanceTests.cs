using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using System.Text.Json;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Serialization;
using JobMaster.Sdk.Utils;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.RecurringSchedules;

public abstract class RepositoryRecurringSchedulesConformanceTests<TFixture>
    where TFixture : RepositoryFixtureBase
{
    protected TFixture Fixture { get; }

    protected RepositoryRecurringSchedulesConformanceTests(TFixture fixture)
    {
        Fixture = fixture;
    }

    [Fact]
    public async Task AddAndGet_ShouldRoundTrip_AllProperties()
    {
        var now = DateTime.UtcNow;

        var schedule = NewSchedule(jobDefinitionId: "def-rt-" + JobMasterRandomUtil.NewGuid4());
        schedule.ExpressionTypeId = NeverRecursExprCompiler.TypeId;
        schedule.Expression = string.Empty;

        schedule.StaticDefinitionId = "static-" + JobMasterRandomUtil.NewGuid4();
        schedule.ProfileId = "profile-" + JobMasterRandomUtil.NewGuid4();

        schedule.Status = RecurringScheduleStatus.PendingSave;
        schedule.RecurringScheduleType = RecurringScheduleType.Static;

        schedule.TerminatedAt = now.AddMinutes(-5);
        schedule.MsgData = "{\"x\":1}";
        schedule.Metadata = "{\"s\":\"alpha\",\"n\":10,\"dt\":\"2025-01-01T00:00:00Z\"}";

        schedule.Priority = JobMasterPriority.High;
        schedule.MaxNumberOfRetries = 7;
        schedule.Timeout = TimeSpan.FromSeconds(42);

        schedule.BucketId = "bucket-1";
        schedule.AgentConnectionId = Fixture.AgentConnectionId;
        schedule.AgentWorkerId = "worker-1";

        schedule.PartitionLockId = JobMasterRandomUtil.NewGuid4();
        schedule.PartitionLockExpiresAt = now.AddMinutes(30);

        schedule.HostId = new JobMaster.Sdk.Abstractions.Models.Hosts.HostId("host-" + JobMasterRandomUtil.NewGuid4().ToString("N"), "test-host-" + JobMasterRandomUtil.NewGuid4().ToString("N"));

        schedule.CreatedAt = now;
        schedule.StartAfter = now.AddMinutes(-10);
        schedule.EndBefore = now.AddDays(1);
        schedule.LastPlanCoverageUntil = now.AddHours(2);
        schedule.LastExecutedPlan = now.AddHours(-1);
        schedule.HasFailedOnLastPlanExecution = true;
        schedule.IsJobCancellationPending = true;
        schedule.StaticDefinitionLastEnsured = now.AddMinutes(-2);
        schedule.WorkerLane = "LANE_RT";

        await Fixture.MasterRecurringSchedules.AddAsync(schedule);

        var fromDb = await Fixture.MasterRecurringSchedules.GetAsync(schedule.Id);
        Assert.NotNull(fromDb);

        AssertScheduleEquivalent(schedule, fromDb!);
    }

    [Fact]
    public async Task AddAndGet_ShouldRoundTrip_AllMetadataTypes()
    {
        var schedule = NewSchedule(jobDefinitionId: "def-meta-rt-" + JobMasterRandomUtil.NewGuid4());
        schedule.ExpressionTypeId = NeverRecursExprCompiler.TypeId;
        schedule.Expression = string.Empty;

        var guid = Guid.Parse("8e8fd3b4-1c3b-4a2b-9d86-3c28b7c7f7b1");
        var dt = new DateTime(2025, 06, 15, 10, 30, 0, DateTimeKind.Utc);

        var metadata = WritableMetadata.New()
            .SetStringValue("str", "hello world")
            .SetIntValue("int", 42)
            .SetLongValue("long", 9999999999L)
            .SetShortValue("short", (short)123)
            .SetByteValue("byte", (byte)7)
            .SetCharValue("char", 'Z')
            .SetBoolValue("bool", true)
            .SetDoubleValue("double", 3.14159)
            .SetDecimalValue("decimal", 123.456m)
            .SetDateTimeValue("dt", dt)
            .SetGuidValue("guid", guid);

        schedule.Metadata = KeyValueBagUtil.Serialize(metadata);

        await Fixture.MasterRecurringSchedules.AddAsync(schedule);

        var fromDb = await Fixture.MasterRecurringSchedules.GetAsync(schedule.Id);
        Assert.NotNull(fromDb);

        var recovered = KeyValueBagUtil.DeserializeMetadata(fromDb!.Metadata).ToReadable();

        Assert.Equal("hello world", recovered.GetStringValue("str"));
        Assert.Equal(42, recovered.GetIntValue("int"));
        Assert.Equal(9999999999L, recovered.GetLongValue("long"));
        Assert.Equal((short)123, recovered.GetShortValue("short"));
        Assert.Equal((byte)7, recovered.GetByteValue("byte"));
        Assert.Equal('Z', recovered.GetCharValue("char"));
        Assert.True(recovered.GetBoolValue("bool"));
        Assert.Equal(3.14159, recovered.GetDoubleValue("double"), 5);
        Assert.Equal(123.456m, recovered.GetDecimalValue("decimal"));
        Assert.Equal(dt, recovered.GetDateTimeValue("dt"));
        Assert.Equal(DateTimeKind.Utc, recovered.GetDateTimeValue("dt").Kind);
        Assert.Equal(guid, recovered.GetGuidValue("guid"));
    }

    [Fact]
    public async Task Update_ShouldPersistChanges()
    {
        var schedule = NewSchedule(jobDefinitionId: "def-upd-" + JobMasterRandomUtil.NewGuid4());
        schedule.Metadata = "{\"original\":\"meta\"}";
        await Fixture.MasterRecurringSchedules.AddAsync(schedule);

        var updated = Clone(schedule);
        var originalVersion = schedule.Version;
        updated.JobDefinitionId = schedule.JobDefinitionId + "-updated";
        updated.ProfileId = "profile-updated";
        updated.Status = RecurringScheduleStatus.Canceled;
        updated.RecurringScheduleType = RecurringScheduleType.Dynamic;
        updated.TerminatedAt = DateTime.UtcNow;
        updated.MsgData = "{\"y\":2}";
        updated.Priority = JobMasterPriority.Low;
        updated.MaxNumberOfRetries = 2;
        updated.Timeout = TimeSpan.FromSeconds(9);
        updated.BucketId = "bucket-upd";
        updated.AgentConnectionId = Fixture.AgentConnectionId;
        updated.AgentWorkerId = "worker-upd";
        updated.PartitionLockId = JobMasterRandomUtil.NewGuid4();
        updated.PartitionLockExpiresAt = DateTime.UtcNow.AddMinutes(10);
        updated.HostId = new JobMaster.Sdk.Abstractions.Models.Hosts.HostId("host-" + JobMasterRandomUtil.NewGuid4().ToString("N"), "updated-host-" + JobMasterRandomUtil.NewGuid4().ToString("N"));
        updated.StartAfter = DateTime.UtcNow.AddHours(-1);
        updated.EndBefore = DateTime.UtcNow.AddHours(5);
        updated.LastPlanCoverageUntil = DateTime.UtcNow.AddHours(3);
        updated.LastExecutedPlan = DateTime.UtcNow.AddMinutes(-30);
        updated.HasFailedOnLastPlanExecution = false;
        updated.IsJobCancellationPending = false;
        updated.StaticDefinitionLastEnsured = DateTime.UtcNow.AddMinutes(-1);
        updated.WorkerLane = "LANE_UPD";
        updated.Metadata = "{\"should\":\"not-persist\"}";

        await Fixture.MasterRecurringSchedules.UpdateAsync(updated);

        var fromDb = await Fixture.MasterRecurringSchedules.GetAsync(schedule.Id);
        Assert.NotNull(fromDb);

        // Metadata is immutable through UpdateAsync — original value must be preserved
        AssertJsonEquivalent(schedule.Metadata, fromDb!.Metadata);
        Assert.NotEqual(updated.Metadata, fromDb!.Metadata);

        // Restore metadata to original for the full equivalence check on all other fields
        updated.Metadata = schedule.Metadata;
        AssertScheduleEquivalent(updated, fromDb!);

        // Version should change on update
        Assert.False(string.IsNullOrEmpty(fromDb!.Version));
        Assert.NotEqual(originalVersion, fromDb!.Version);
    }

    [Fact]
    public async Task Update_ShouldThrow_OnVersionConflict_WhenConcurrent()
    {
        var schedule = NewSchedule(jobDefinitionId: "def-conflict-" + JobMasterRandomUtil.NewGuid4());
        await Fixture.MasterRecurringSchedules.AddAsync(schedule);

        // Load two separate copies to simulate concurrent updates
        var copyA = await Fixture.MasterRecurringSchedules.GetAsync(schedule.Id);
        var copyB = await Fixture.MasterRecurringSchedules.GetAsync(schedule.Id);
        Assert.NotNull(copyA);
        Assert.NotNull(copyB);

        // First update succeeds and advances the version
        copyA!.JobDefinitionId = copyA.JobDefinitionId + "-A";
        await Fixture.MasterRecurringSchedules.UpdateAsync(copyA);

        // Second update uses stale version — should throw
        copyB!.JobDefinitionId = copyB.JobDefinitionId + "-B";
        await Assert.ThrowsAsync<JobMasterVersionConflictException>(() =>
            Fixture.MasterRecurringSchedules.UpdateAsync(copyB));
    }

    [Fact]
    public async Task Update_ShouldThrow_WhenVersionMismatch()
    {
        var schedule = NewSchedule(jobDefinitionId: "def-mismatch-" + JobMasterRandomUtil.NewGuid4());
        await Fixture.MasterRecurringSchedules.AddAsync(schedule);

        var current = await Fixture.MasterRecurringSchedules.GetAsync(schedule.Id);
        Assert.NotNull(current);

        var stale = Clone(current!);
        stale.Version = JobMasterRandomUtil.NewGuid4().ToString("N");
        stale.JobDefinitionId = stale.JobDefinitionId + "-STALE";

        await Assert.ThrowsAsync<JobMasterVersionConflictException>(() =>
            Fixture.MasterRecurringSchedules.UpdateAsync(stale));
    }

    [Fact]
    public async Task GetByStaticId_ShouldReturnOnly_StaticSchedules()
    {
        var staticId = "static-" + JobMasterRandomUtil.NewGuid4();

        var matching = NewSchedule(jobDefinitionId: "def-static-" + JobMasterRandomUtil.NewGuid4());
        matching.StaticDefinitionId = staticId;
        matching.RecurringScheduleType = RecurringScheduleType.Static;

        var nonStatic = NewSchedule(jobDefinitionId: "def-nonstatic-" + JobMasterRandomUtil.NewGuid4());
        nonStatic.StaticDefinitionId = staticId;
        nonStatic.RecurringScheduleType = RecurringScheduleType.Dynamic;

        await Fixture.MasterRecurringSchedules.AddAsync(matching);
        await Fixture.MasterRecurringSchedules.AddAsync(nonStatic);

        var got = Fixture.MasterRecurringSchedules.GetByStaticId(staticId);
        Assert.NotNull(got);
        Assert.Equal(matching.Id, got!.Id);
    }

    [Fact]
    public async Task Query_And_Count_ShouldBeConsistent_ForCommonFilters()
    {
        var baseTime = DateTime.UtcNow;
        var defA = "defA-" + JobMasterRandomUtil.NewGuid4();
        var defB = "defB-" + JobMasterRandomUtil.NewGuid4();

        var s1 = NewSchedule(jobDefinitionId: defA);
        s1.Status = RecurringScheduleStatus.Active;
        s1.ProfileId = "p1";
        s1.WorkerLane = "L1";
        s1.LastPlanCoverageUntil = baseTime.AddHours(1);

        var s2 = NewSchedule(jobDefinitionId: defA);
        s2.Status = RecurringScheduleStatus.Active;
        s2.ProfileId = "p1";
        s2.WorkerLane = "L2";
        s2.LastPlanCoverageUntil = baseTime.AddHours(2);

        var s3 = NewSchedule(jobDefinitionId: defB);
        s3.Status = RecurringScheduleStatus.Inactive;
        s3.ProfileId = "p2";
        s3.WorkerLane = "L1";
        s3.LastPlanCoverageUntil = baseTime.AddHours(3);
        s3.TerminatedAt = baseTime;

        await Fixture.MasterRecurringSchedules.AddAsync(s1);
        await Fixture.MasterRecurringSchedules.AddAsync(s2);
        await Fixture.MasterRecurringSchedules.AddAsync(s3);

        var c = new RecurringScheduleQueryCriteria
        {
            JobDefinitionId = defA,
            Status = RecurringScheduleStatus.Active,
            ProfileId = "p1",
            CountLimit = 100,
            Offset = 0
        };

        var queried = await Fixture.MasterRecurringSchedules.QueryAsync(c);
        var count = Fixture.MasterRecurringSchedules.Count(c);

        Assert.Equal(count, queried.Count);
        Assert.All(queried, x => Assert.Equal(defA, x.JobDefinitionId));
        Assert.All(queried, x => Assert.Equal(RecurringScheduleStatus.Active, x.Status));
        Assert.All(queried, x => Assert.Equal("p1", x.ProfileId));
    }

    [Fact]
    public async Task ProbeCountForAcquire_ShouldExclude_ActivelyLockedSchedules_And_Include_ExpiredLocks()
    {
        var def = "defProbe-" + JobMasterRandomUtil.NewGuid4();
        var now = DateTime.UtcNow;

        var unlocked = NewSchedule(jobDefinitionId: def);

        var activeLock = NewSchedule(jobDefinitionId: def);
        activeLock.PartitionLockId = JobMasterRandomUtil.NewGuid4();
        activeLock.PartitionLockExpiresAt = now.AddMinutes(30);

        var expiredLock = NewSchedule(jobDefinitionId: def);
        expiredLock.PartitionLockId = JobMasterRandomUtil.NewGuid4();
        expiredLock.PartitionLockExpiresAt = now.AddMinutes(-10);

        await Fixture.MasterRecurringSchedules.AddAsync(unlocked);
        await Fixture.MasterRecurringSchedules.AddAsync(activeLock);
        await Fixture.MasterRecurringSchedules.AddAsync(expiredLock);

        var criteria = new RecurringScheduleQueryCriteria { JobDefinitionId = def, Status = RecurringScheduleStatus.Active, CountLimit = 100 };
        var count = await Fixture.MasterRecurringSchedules.ProbeCountForAcquireAsync(criteria);

        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Query_ShouldSupport_StartAfter_And_EndBefore_Ranges_WithNulls()
    {
        var def = "defRanges-" + JobMasterRandomUtil.NewGuid4();
        var baseTime = DateTime.UtcNow;

        var nulls = NewSchedule(jobDefinitionId: def);
        nulls.StartAfter = null;
        nulls.EndBefore = null;

        var inside = NewSchedule(jobDefinitionId: def);
        inside.StartAfter = baseTime.AddHours(-1);
        inside.EndBefore = baseTime.AddHours(10);

        var outside = NewSchedule(jobDefinitionId: def);
        outside.StartAfter = baseTime.AddHours(100);
        outside.EndBefore = baseTime.AddHours(200);

        await Fixture.MasterRecurringSchedules.AddAsync(nulls);
        await Fixture.MasterRecurringSchedules.AddAsync(inside);
        await Fixture.MasterRecurringSchedules.AddAsync(outside);

        var c = new RecurringScheduleQueryCriteria
        {
            JobDefinitionId = def,
            StartAfterTo = baseTime,
            EndBeforeTo = baseTime.AddHours(20),
            CountLimit = 100
        };

        var queried = await Fixture.MasterRecurringSchedules.QueryAsync(c);

        Assert.Contains(queried, x => x.Id == nulls.Id);
        Assert.Contains(queried, x => x.Id == inside.Id);
        Assert.DoesNotContain(queried, x => x.Id == outside.Id);
    }

    [Fact]
    public async Task Query_ShouldSupport_CoverageUntil_Filter_WithNulls()
    {
        var def = "defCoverage-" + JobMasterRandomUtil.NewGuid4();
        var baseTime = DateTime.UtcNow;

        var nullCoverage = NewSchedule(jobDefinitionId: def);
        nullCoverage.LastPlanCoverageUntil = null;

        var covered = NewSchedule(jobDefinitionId: def);
        covered.LastPlanCoverageUntil = baseTime.AddHours(1);

        var over = NewSchedule(jobDefinitionId: def);
        over.LastPlanCoverageUntil = baseTime.AddHours(50);

        await Fixture.MasterRecurringSchedules.AddAsync(nullCoverage);
        await Fixture.MasterRecurringSchedules.AddAsync(covered);
        await Fixture.MasterRecurringSchedules.AddAsync(over);

        var c = new RecurringScheduleQueryCriteria
        {
            JobDefinitionId = def,
            CoverageUntil = baseTime.AddHours(2),
            CountLimit = 100
        };

        var queried = await Fixture.MasterRecurringSchedules.QueryAsync(c);

        Assert.Contains(queried, x => x.Id == nullCoverage.Id);
        Assert.Contains(queried, x => x.Id == covered.Id);
        Assert.DoesNotContain(queried, x => x.Id == over.Id);
    }

    [Fact]
    public async Task Query_ShouldSupport_IsJobCancellationPending_Filter()
    {
        var def = "defCancel-" + JobMasterRandomUtil.NewGuid4();

        var a = NewSchedule(jobDefinitionId: def);
        a.IsJobCancellationPending = true;

        var b = NewSchedule(jobDefinitionId: def);
        b.IsJobCancellationPending = false;

        await Fixture.MasterRecurringSchedules.AddAsync(a);
        await Fixture.MasterRecurringSchedules.AddAsync(b);

        var c = new RecurringScheduleQueryCriteria
        {
            JobDefinitionId = def,
            IsJobCancellationPending = true,
            CountLimit = 100
        };

        var queried = await Fixture.MasterRecurringSchedules.QueryAsync(c);
        Assert.Contains(queried, x => x.Id == a.Id);
        Assert.DoesNotContain(queried, x => x.Id == b.Id);
    }

    [Fact]
    public async Task Query_ShouldSupport_CanceledOrInactive_Filter()
    {
        var def = "defCanceledOrInactive-" + JobMasterRandomUtil.NewGuid4();

        var active = NewSchedule(jobDefinitionId: def);
        active.Status = RecurringScheduleStatus.Active;

        var canceled = NewSchedule(jobDefinitionId: def);
        canceled.Status = RecurringScheduleStatus.Canceled;
        canceled.TerminatedAt = DateTime.UtcNow;

        var inactive = NewSchedule(jobDefinitionId: def);
        inactive.Status = RecurringScheduleStatus.Inactive;
        inactive.TerminatedAt = DateTime.UtcNow;

        await Fixture.MasterRecurringSchedules.AddAsync(active);
        await Fixture.MasterRecurringSchedules.AddAsync(canceled);
        await Fixture.MasterRecurringSchedules.AddAsync(inactive);

        var c = new RecurringScheduleQueryCriteria
        {
            JobDefinitionId = def,
            CanceledOrInactive = true,
            CountLimit = 100
        };

        var queried = await Fixture.MasterRecurringSchedules.QueryAsync(c);
        Assert.Contains(queried, x => x.Id == canceled.Id);
        Assert.Contains(queried, x => x.Id == inactive.Id);
        Assert.DoesNotContain(queried, x => x.Id == active.Id);
    }

    [Fact]
    public async Task Query_ShouldSupport_RecurringScheduleType_ProfileId_And_WorkerLane()
    {
        var def = "defTypeProfileLane-" + JobMasterRandomUtil.NewGuid4();

        var a = NewSchedule(jobDefinitionId: def);
        a.RecurringScheduleType = RecurringScheduleType.Static;
        a.ProfileId = "p1";
        a.WorkerLane = "L1";

        var b = NewSchedule(jobDefinitionId: def);
        b.RecurringScheduleType = RecurringScheduleType.Dynamic;
        b.ProfileId = "p2";
        b.WorkerLane = "L2";

        await Fixture.MasterRecurringSchedules.AddAsync(a);
        await Fixture.MasterRecurringSchedules.AddAsync(b);

        var c = new RecurringScheduleQueryCriteria
        {
            JobDefinitionId = def,
            RecurringScheduleType = RecurringScheduleType.Static,
            ProfileId = "p1",
            WorkerLane = "L1",
            CountLimit = 100
        };

        var queried = await Fixture.MasterRecurringSchedules.QueryAsync(c);
        Assert.Contains(queried, x => x.Id == a.Id);
        Assert.DoesNotContain(queried, x => x.Id == b.Id);
    }

    [Fact]
    public async Task Query_ShouldSupport_MetadataFilters_AllOperations_And_Types()
    {
        var def = "defMeta-" + JobMasterRandomUtil.NewGuid4();

        var t0 = new DateTime(2025, 01, 01, 0, 0, 0, DateTimeKind.Utc);

        var a = NewSchedule(jobDefinitionId: def);
        a.Metadata = "{\"s\":\"alpha\",\"n\":10,\"dt\":\"2025-01-01T00:00:00Z\"}";

        var b = NewSchedule(jobDefinitionId: def);
        b.Metadata = "{\"s\":\"alphabet\",\"n\":20,\"dt\":\"2025-01-02T00:00:00Z\"}";

        var cSch = NewSchedule(jobDefinitionId: def);
        cSch.Metadata = "{\"s\":\"beta\",\"n\":30,\"dt\":\"2025-01-03T00:00:00Z\"}";

        await Fixture.MasterRecurringSchedules.AddAsync(a);
        await Fixture.MasterRecurringSchedules.AddAsync(b);
        await Fixture.MasterRecurringSchedules.AddAsync(cSch);

        // String operations
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "s", Operation = GenericFilterOperation.Eq, Value = "alpha" }, a.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "s", Operation = GenericFilterOperation.Neq, Value = "alpha" }, b.Id, cSch.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "s", Operation = GenericFilterOperation.In, Values = new object?[] { "alpha", "beta" } }, a.Id, cSch.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "s", Operation = GenericFilterOperation.Contains, Value = "lph" }, a.Id, b.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "s", Operation = GenericFilterOperation.StartsWith, Value = "alph" }, a.Id, b.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "s", Operation = GenericFilterOperation.EndsWith, Value = "bet" }, b.Id);

        // Numeric operations
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.Eq, Value = 20 }, b.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.Neq, Value = 20 }, a.Id, cSch.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.In, Values = new object?[] { 10, 30 } }, a.Id, cSch.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.Gt, Value = 10 }, b.Id, cSch.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.Gte, Value = 20 }, b.Id, cSch.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.Lt, Value = 30 }, a.Id, b.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.Lte, Value = 20 }, a.Id, b.Id);

        // DateTime operations
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.Eq, Value = t0.AddDays(1) }, b.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.Neq, Value = t0.AddDays(1) }, a.Id, cSch.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.In, Values = new object?[] { t0, t0.AddDays(2) } }, a.Id, cSch.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.Gt, Value = t0 }, b.Id, cSch.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.Gte, Value = t0.AddDays(2) }, cSch.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.Lt, Value = t0.AddDays(2) }, a.Id, b.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.Lte, Value = t0.AddDays(1) }, a.Id, b.Id);
    }

    [Fact]
    public async Task Query_ShouldSupport_CountLimit_And_Offset_OrderIsDeterministic()
    {
        var def = "defPaging-" + JobMasterRandomUtil.NewGuid4();
        var baseTime = DateTime.UtcNow;

        var list = new List<RecurringScheduleRawModel>();
        for (var i = 0; i < 5; i++)
        {
            var s = NewSchedule(jobDefinitionId: def);
            s.LastPlanCoverageUntil = baseTime.AddHours(i); // ordering uses DESC
            s.CreatedAt = baseTime.AddMinutes(i);
            list.Add(s);
            await Fixture.MasterRecurringSchedules.AddAsync(s);
        }

        var c = new RecurringScheduleQueryCriteria
        {
            JobDefinitionId = def,
            CountLimit = 2,
            Offset = 1
        };

        var queried = await Fixture.MasterRecurringSchedules.QueryAsync(c);
        Assert.Equal(2, queried.Count);

        var ordered = list
            .OrderByDescending(x => x.LastPlanCoverageUntil)
            .ThenBy(x => x.CreatedAt)
            .Select(x => x.Id)
            .ToList();

        var expected = ordered.Skip(1).Take(2).ToList();
        Assert.Equal(expected, queried.Select(x => x.Id).ToList());
    }

    internal async Task AssertMetadataFilter(string jobDefinitionId, GenericRecordValueFilter filter, params Guid[] expectedIds)
    {
        var criteria = new RecurringScheduleQueryCriteria
        {
            JobDefinitionId = jobDefinitionId,
            CountLimit = 100,
            MetadataFilters = new List<GenericRecordValueFilter> { filter }
        };

        var queried = await Fixture.MasterRecurringSchedules.QueryAsync(criteria);
        var ids = queried.Select(x => x.Id).ToHashSet();

        Assert.Equal(expectedIds.OrderBy(x => x).ToList(), ids.OrderBy(x => x).ToList());
    }

    // -----------------------------------------------------------------------
    // BulkInsertIfNotExistsAsync conformance tests
    // -----------------------------------------------------------------------

    [Fact]
    public async Task BulkInsertIfNotExists_ShouldInsert_NewTerminatedSchedules()
    {
        var def = "defBulkInsertNew-" + JobMasterRandomUtil.NewGuid4();

        var s1 = NewSchedule(def);
        s1.Status = RecurringScheduleStatus.Canceled;
        s1.TerminatedAt = DateTime.UtcNow;
        s1.Metadata = "{\"testIdentifier\":\"bulk-insert-s1\"}";

        var s2 = NewSchedule(def);
        s2.Status = RecurringScheduleStatus.Completed;
        s2.TerminatedAt = DateTime.UtcNow;
        s2.Metadata = "{\"testIdentifier\":\"bulk-insert-s2\"}";

        await Fixture.MasterRecurringSchedules.BulkInsertIfNotExistsAsync(new[] { s1, s2 });

        var fromDb1 = await Fixture.MasterRecurringSchedules.GetAsync(s1.Id);
        var fromDb2 = await Fixture.MasterRecurringSchedules.GetAsync(s2.Id);

        Assert.NotNull(fromDb1);
        Assert.NotNull(fromDb2);
        Assert.Equal(RecurringScheduleStatus.Canceled, fromDb1!.Status);
        Assert.Equal(RecurringScheduleStatus.Completed, fromDb2!.Status);
        Assert.False(string.IsNullOrEmpty(fromDb1.Version));
        Assert.False(string.IsNullOrEmpty(fromDb2.Version));

        AssertJsonEquivalent(s1.Metadata, fromDb1.Metadata);
        AssertJsonEquivalent(s2.Metadata, fromDb2.Metadata);

        var queried = await Fixture.MasterRecurringSchedules.QueryAsync(new RecurringScheduleQueryCriteria
        {
            JobDefinitionId = def,
            CountLimit = 100,
            MetadataFilters = new List<GenericRecordValueFilter>
            {
                new() { Key = "testIdentifier", Operation = GenericFilterOperation.Eq, Value = "bulk-insert-s1" }
            }
        });
        Assert.Equal(s1.Id, Assert.Single(queried).Id);
    }

    [Fact]
    public async Task BulkInsertIfNotExists_ShouldPersist_AllMetadataAndMsgDataTypes()
    {
        var def = "defBulkInsertMetaMsg-" + JobMasterRandomUtil.NewGuid4();

        var guid1 = Guid.Parse("8e8fd3b4-1c3b-4a2b-9d86-3c28b7c7f7b1");
        var dt1 = new DateTime(2025, 06, 15, 10, 30, 0, DateTimeKind.Utc);
        var guid2 = Guid.Parse("9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d");
        var dt2 = new DateTime(2025, 03, 10, 08, 15, 0, DateTimeKind.Utc);

        var s1 = NewSchedule(def);
        s1.Status = RecurringScheduleStatus.Canceled;
        s1.TerminatedAt = DateTime.UtcNow;
        s1.Metadata = KeyValueBagUtil.Serialize(WritableMetadata.New()
            .SetStringValue("str", "s1-meta")
            .SetIntValue("int", 1)
            .SetLongValue("long", 111L)
            .SetShortValue("short", (short)11)
            .SetByteValue("byte", (byte)1)
            .SetCharValue("char", 'A')
            .SetBoolValue("bool", true)
            .SetDoubleValue("double", 1.1)
            .SetDecimalValue("decimal", 1.11m)
            .SetDateTimeValue("dt", dt1)
            .SetGuidValue("guid", guid1));
        s1.MsgData = KeyValueBagUtil.Serialize(new MessageData()
            .SetStringValue("mstr", "s1-msg")
            .SetIntValue("mint", 10)
            .SetBoolValue("mbool", true)
            .SetDateTimeValue("mdt", dt1)
            .SetGuidValue("mguid", guid1));

        var s2 = NewSchedule(def);
        s2.Status = RecurringScheduleStatus.Completed;
        s2.TerminatedAt = DateTime.UtcNow;
        s2.Metadata = KeyValueBagUtil.Serialize(WritableMetadata.New()
            .SetStringValue("str", "s2-meta")
            .SetIntValue("int", 2)
            .SetLongValue("long", 222L)
            .SetShortValue("short", (short)22)
            .SetByteValue("byte", (byte)2)
            .SetCharValue("char", 'B')
            .SetBoolValue("bool", false)
            .SetDoubleValue("double", 2.2)
            .SetDecimalValue("decimal", 2.22m)
            .SetDateTimeValue("dt", dt2)
            .SetGuidValue("guid", guid2));
        s2.MsgData = KeyValueBagUtil.Serialize(new MessageData()
            .SetStringValue("mstr", "s2-msg")
            .SetIntValue("mint", 20)
            .SetBoolValue("mbool", false)
            .SetDateTimeValue("mdt", dt2)
            .SetGuidValue("mguid", guid2));

        await Fixture.MasterRecurringSchedules.BulkInsertIfNotExistsAsync(new[] { s1, s2 });

        var fromDb1 = await Fixture.MasterRecurringSchedules.GetAsync(s1.Id);
        var fromDb2 = await Fixture.MasterRecurringSchedules.GetAsync(s2.Id);
        Assert.NotNull(fromDb1);
        Assert.NotNull(fromDb2);

        var meta1 = KeyValueBagUtil.DeserializeMetadata(fromDb1!.Metadata).ToReadable();
        Assert.Equal("s1-meta", meta1.GetStringValue("str"));
        Assert.Equal(1, meta1.GetIntValue("int"));
        Assert.Equal(111L, meta1.GetLongValue("long"));
        Assert.Equal((short)11, meta1.GetShortValue("short"));
        Assert.Equal((byte)1, meta1.GetByteValue("byte"));
        Assert.Equal('A', meta1.GetCharValue("char"));
        Assert.True(meta1.GetBoolValue("bool"));
        Assert.Equal(1.1, meta1.GetDoubleValue("double"));
        Assert.Equal(1.11m, meta1.GetDecimalValue("decimal"));
        Assert.Equal(dt1, meta1.GetDateTimeValue("dt"));
        Assert.Equal(guid1, meta1.GetGuidValue("guid"));

        var msg1 = KeyValueBagUtil.DeserializeMessageData(fromDb1.MsgData).ToReadable();
        Assert.Equal("s1-msg", msg1.GetStringValue("mstr"));
        Assert.Equal(10, msg1.GetIntValue("mint"));
        Assert.True(msg1.GetBoolValue("mbool"));
        Assert.Equal(dt1, msg1.GetDateTimeValue("mdt"));
        Assert.Equal(guid1, msg1.GetGuidValue("mguid"));

        var meta2 = KeyValueBagUtil.DeserializeMetadata(fromDb2!.Metadata).ToReadable();
        Assert.Equal("s2-meta", meta2.GetStringValue("str"));
        Assert.Equal(2, meta2.GetIntValue("int"));
        Assert.Equal(222L, meta2.GetLongValue("long"));
        Assert.Equal((short)22, meta2.GetShortValue("short"));
        Assert.Equal((byte)2, meta2.GetByteValue("byte"));
        Assert.Equal('B', meta2.GetCharValue("char"));
        Assert.False(meta2.GetBoolValue("bool"));
        Assert.Equal(2.2, meta2.GetDoubleValue("double"));
        Assert.Equal(2.22m, meta2.GetDecimalValue("decimal"));
        Assert.Equal(dt2, meta2.GetDateTimeValue("dt"));
        Assert.Equal(guid2, meta2.GetGuidValue("guid"));

        var msg2 = KeyValueBagUtil.DeserializeMessageData(fromDb2.MsgData).ToReadable();
        Assert.Equal("s2-msg", msg2.GetStringValue("mstr"));
        Assert.Equal(20, msg2.GetIntValue("mint"));
        Assert.False(msg2.GetBoolValue("mbool"));
        Assert.Equal(dt2, msg2.GetDateTimeValue("mdt"));
        Assert.Equal(guid2, msg2.GetGuidValue("mguid"));
    }

    [Fact]
    public async Task BulkInsertIfNotExists_ShouldLeaveExistingSchedules_Untouched()
    {
        var def = "defBulkInsertExisting-" + JobMasterRandomUtil.NewGuid4();

        var existing = NewSchedule(def);
        await Fixture.MasterRecurringSchedules.AddAsync(existing);
        var originalVersion = (await Fixture.MasterRecurringSchedules.GetAsync(existing.Id))!.Version;

        var conflicting = Clone(existing);
        conflicting.WorkerLane = "SHOULD_NOT_PERSIST";
        conflicting.Metadata = "{\"should\":\"not-persist\"}";

        var brandNew = NewSchedule(def);
        brandNew.Status = RecurringScheduleStatus.Canceled;
        brandNew.TerminatedAt = DateTime.UtcNow;

        await Fixture.MasterRecurringSchedules.BulkInsertIfNotExistsAsync(new[] { conflicting, brandNew });

        var fromDbExisting = await Fixture.MasterRecurringSchedules.GetAsync(existing.Id);
        Assert.NotNull(fromDbExisting);
        Assert.Equal(originalVersion, fromDbExisting!.Version);
        Assert.NotEqual("SHOULD_NOT_PERSIST", fromDbExisting.WorkerLane);
        Assert.DoesNotContain("not-persist", fromDbExisting.Metadata ?? string.Empty);

        var fromDbNew = await Fixture.MasterRecurringSchedules.GetAsync(brandNew.Id);
        Assert.NotNull(fromDbNew);
        Assert.Equal(RecurringScheduleStatus.Canceled, fromDbNew!.Status);
    }

    [Fact]
    public async Task BulkInsertIfNotExists_EmptyList_ShouldNoOp()
    {
        await Fixture.MasterRecurringSchedules.BulkInsertIfNotExistsAsync(Array.Empty<RecurringScheduleRawModel>());
    }

    internal virtual RecurringScheduleRawModel NewSchedule(string? jobDefinitionId = null)
    {
        var now = DateTime.UtcNow;
        return new RecurringScheduleRawModel(Fixture.ClusterId)
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            ExpressionTypeId = NeverRecursExprCompiler.TypeId,
            Expression = string.Empty,
            JobDefinitionId = jobDefinitionId ?? ("def-" + JobMasterRandomUtil.NewGuid4()),
            Status = RecurringScheduleStatus.Active,
            RecurringScheduleType = RecurringScheduleType.Dynamic,
            MsgData = "{}",
            CreatedAt = now,
            Metadata = "{}",
        };
    }

    private static void AssertScheduleEquivalent(RecurringScheduleRawModel expected, RecurringScheduleRawModel actual)
    {
        Assert.Equal(expected.ClusterId, actual.ClusterId);

        Assert.Equal(expected.Id, actual.Id);
        Assert.Equal(expected.Expression, actual.Expression);
        Assert.Equal(expected.ExpressionTypeId, actual.ExpressionTypeId);
        Assert.Equal(expected.JobDefinitionId, actual.JobDefinitionId);
        Assert.Equal(expected.StaticDefinitionId, actual.StaticDefinitionId);
        Assert.Equal(expected.ProfileId, actual.ProfileId);
        Assert.Equal(expected.Status, actual.Status);
        Assert.Equal(expected.RecurringScheduleType, actual.RecurringScheduleType);

        AssertDateTimeEquivalent(ToUtcN(expected.StaticDefinitionLastEnsured), ToUtcN(actual.StaticDefinitionLastEnsured));
        AssertDateTimeEquivalent(ToUtcN(expected.TerminatedAt), ToUtcN(actual.TerminatedAt));

        AssertJsonEquivalent(expected.MsgData, actual.MsgData);
        AssertJsonEquivalent(expected.Metadata, actual.Metadata);

        Assert.Equal(expected.Priority, actual.Priority);
        Assert.Equal(expected.MaxNumberOfRetries, actual.MaxNumberOfRetries);
        Assert.Equal(expected.Timeout, actual.Timeout);

        Assert.Equal(expected.BucketId, actual.BucketId);
        Assert.Equal(expected.AgentConnectionId?.IdValue, actual.AgentConnectionId?.IdValue);
        Assert.Equal(expected.AgentWorkerId, actual.AgentWorkerId);

        Assert.Equal(expected.PartitionLockId, actual.PartitionLockId);
        AssertDateTimeEquivalent(ToUtcN(expected.PartitionLockExpiresAt), ToUtcN(actual.PartitionLockExpiresAt));

        AssertDateTimeEquivalent(ToUtc(expected.CreatedAt), ToUtc(actual.CreatedAt));
        AssertDateTimeEquivalent(ToUtcN(expected.StartAfter), ToUtcN(actual.StartAfter));
        AssertDateTimeEquivalent(ToUtcN(expected.EndBefore), ToUtcN(actual.EndBefore));
        AssertDateTimeEquivalent(ToUtcN(expected.LastPlanCoverageUntil), ToUtcN(actual.LastPlanCoverageUntil));
        AssertDateTimeEquivalent(ToUtcN(expected.LastExecutedPlan), ToUtcN(actual.LastExecutedPlan));

        Assert.Equal(expected.HasFailedOnLastPlanExecution, actual.HasFailedOnLastPlanExecution);
        Assert.Equal(expected.IsJobCancellationPending, actual.IsJobCancellationPending);
        Assert.Equal(expected.WorkerLane, actual.WorkerLane);
        Assert.Equal(expected.HostId?.IdValue, actual.HostId?.IdValue);
        Assert.Equal(expected.HostId?.HostDisplayName, actual.HostId?.HostDisplayName);
    }

    private static RecurringScheduleRawModel Clone(RecurringScheduleRawModel s)
    {
        return new RecurringScheduleRawModel(s.ClusterId)
        {
            Id = s.Id,
            Expression = s.Expression,
            ExpressionTypeId = s.ExpressionTypeId,
            JobDefinitionId = s.JobDefinitionId,
            StaticDefinitionId = s.StaticDefinitionId,
            ProfileId = s.ProfileId,
            Status = s.Status,
            RecurringScheduleType = s.RecurringScheduleType,
            TerminatedAt = s.TerminatedAt,
            MsgData = s.MsgData,
            Metadata = s.Metadata,
            Priority = s.Priority,
            MaxNumberOfRetries = s.MaxNumberOfRetries,
            Timeout = s.Timeout,
            BucketId = s.BucketId,
            AgentConnectionId = s.AgentConnectionId,
            AgentWorkerId = s.AgentWorkerId,
            PartitionLockId = s.PartitionLockId,
            PartitionLockExpiresAt = s.PartitionLockExpiresAt,
            CreatedAt = s.CreatedAt,
            StartAfter = s.StartAfter,
            EndBefore = s.EndBefore,
            LastPlanCoverageUntil = s.LastPlanCoverageUntil,
            LastExecutedPlan = s.LastExecutedPlan,
            HasFailedOnLastPlanExecution = s.HasFailedOnLastPlanExecution,
            IsJobCancellationPending = s.IsJobCancellationPending,
            StaticDefinitionLastEnsured = s.StaticDefinitionLastEnsured,
            WorkerLane = s.WorkerLane,
            HostId = s.HostId,
            Version = s.Version
        };
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

    private static void AssertJsonEquivalent(string? expectedJson, string? actualJson)
    {
        if (string.IsNullOrWhiteSpace(expectedJson) && string.IsNullOrWhiteSpace(actualJson))
        {
            return;
        }

        Assert.False(string.IsNullOrWhiteSpace(expectedJson));
        Assert.False(string.IsNullOrWhiteSpace(actualJson));

        var expected = InternalJobMasterSerializer.Deserialize<Dictionary<string, object?>>(expectedJson!);
        var actual = InternalJobMasterSerializer.Deserialize<Dictionary<string, object?>>(actualJson!);

        Assert.Equal(expected.Count, actual.Count);
        foreach (var (key, expectedVal) in expected)
        {
            Assert.True(actual.ContainsKey(key));
            var actualVal = actual[key];
            AssertMetadataValueEquivalent(expectedVal, actualVal);
        }
    }

    private static void AssertMetadataValueEquivalent(object? expected, object? actual)
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

    [Fact]
    public async Task PurgeTerminatedAsync_ShouldDelete_OnlyTerminatedSchedulesOlderThanCutoff()
    {
        var def = "defPurge-" + JobMasterRandomUtil.NewGuid4();
        var baseTime = DateTime.UtcNow.AddHours(-10);
        var cutoff = baseTime.AddMinutes(5);

        var oldInactive = NewSchedule(jobDefinitionId: def);
        oldInactive.Status = RecurringScheduleStatus.Inactive;
        oldInactive.TerminatedAt = baseTime.AddMinutes(1);
        oldInactive.CreatedAt = baseTime.AddMinutes(1);

        var oldCanceled = NewSchedule(jobDefinitionId: def);
        oldCanceled.Status = RecurringScheduleStatus.Canceled;
        oldCanceled.TerminatedAt = baseTime.AddMinutes(3);
        oldCanceled.CreatedAt = baseTime.AddMinutes(3);

        var recentInactive = NewSchedule(jobDefinitionId: def);
        recentInactive.Status = RecurringScheduleStatus.Inactive;
        recentInactive.TerminatedAt = baseTime.AddMinutes(10);
        recentInactive.CreatedAt = baseTime.AddMinutes(10);

        var active = NewSchedule(jobDefinitionId: def);
        active.Status = RecurringScheduleStatus.Active;
        active.TerminatedAt = null;
        active.CreatedAt = baseTime.AddMinutes(1);

        await Fixture.MasterRecurringSchedules.AddAsync(oldInactive);
        await Fixture.MasterRecurringSchedules.AddAsync(oldCanceled);
        await Fixture.MasterRecurringSchedules.AddAsync(recentInactive);
        await Fixture.MasterRecurringSchedules.AddAsync(active);

        var deleted = await Fixture.MasterRecurringSchedules.PurgeTerminatedAsync(cutoff, limit: 100);
        Assert.True(deleted >= 2, $"Expected at least 2 deleted, got {deleted}");

        var remaining = await Fixture.MasterRecurringSchedules.QueryAsync(new RecurringScheduleQueryCriteria
        {
            JobDefinitionId = def,
            CountLimit = 100
        });

        Assert.DoesNotContain(remaining, s => s.Id == oldInactive.Id);
        Assert.DoesNotContain(remaining, s => s.Id == oldCanceled.Id);
        Assert.Contains(remaining, s => s.Id == recentInactive.Id);
        Assert.Contains(remaining, s => s.Id == active.Id);
    }

    [Fact]
    public async Task PurgeTerminatedAsync_ShouldRespect_Limit()
    {
        var def = "defPurgeLimit-" + JobMasterRandomUtil.NewGuid4();
        var baseTime = DateTime.UtcNow.AddHours(-10);
        var cutoff = baseTime.AddMinutes(50);

        for (var i = 0; i < 10; i++)
        {
            var s = NewSchedule(jobDefinitionId: def);
            s.Status = RecurringScheduleStatus.Inactive;
            s.TerminatedAt = baseTime.AddMinutes(i);
            s.CreatedAt = baseTime.AddMinutes(i);
            await Fixture.MasterRecurringSchedules.AddAsync(s);
        }

        var deleted = await Fixture.MasterRecurringSchedules.PurgeTerminatedAsync(cutoff, limit: 3);
        Assert.True(deleted <= 3, $"Expected at most 3 deleted, got {deleted}");
        Assert.True(deleted >= 1, $"Expected at least 1 deleted, got {deleted}");

        var remaining = await Fixture.MasterRecurringSchedules.QueryAsync(new RecurringScheduleQueryCriteria
        {
            JobDefinitionId = def,
            CountLimit = 100
        });
        Assert.True(remaining.Count >= 7, $"Expected at least 7 remaining, got {remaining.Count}");
    }

    [Fact]
    public async Task PurgeTerminatedAsync_ShouldNotDelete_ActiveSchedules()
    {
        var def = "defPurgeActive-" + JobMasterRandomUtil.NewGuid4();
        var baseTime = DateTime.UtcNow.AddHours(-10);
        var cutoff = baseTime.AddMinutes(50);

        var activeOld = NewSchedule(jobDefinitionId: def);
        activeOld.Status = RecurringScheduleStatus.Active;
        activeOld.TerminatedAt = null;
        activeOld.CreatedAt = baseTime.AddMinutes(1);

        var pendingSaveOld = NewSchedule(jobDefinitionId: def);
        pendingSaveOld.Status = RecurringScheduleStatus.PendingSave;
        pendingSaveOld.TerminatedAt = null;
        pendingSaveOld.CreatedAt = baseTime.AddMinutes(2);

        await Fixture.MasterRecurringSchedules.AddAsync(activeOld);
        await Fixture.MasterRecurringSchedules.AddAsync(pendingSaveOld);

        var deleted = await Fixture.MasterRecurringSchedules.PurgeTerminatedAsync(cutoff, limit: 100);

        var remaining = await Fixture.MasterRecurringSchedules.QueryAsync(new RecurringScheduleQueryCriteria
        {
            JobDefinitionId = def,
            CountLimit = 100
        });

        Assert.Contains(remaining, s => s.Id == activeOld.Id);
        Assert.Contains(remaining, s => s.Id == pendingSaveOld.Id);
    }

    // -----------------------------------------------------------------------
    // Partition lock regression tests
    // -----------------------------------------------------------------------

    [Fact]
    public async Task Query_ShouldReturn_ActivelyLockedSchedules()
    {
        // Regression: QueryAsync must return locked schedules — no default isLocked filter.
        var def = "defQueryLocked-" + JobMasterRandomUtil.NewGuid4();
        var now = DateTime.UtcNow;

        var locked = NewSchedule(jobDefinitionId: def);
        locked.PartitionLockId = JobMasterRandomUtil.NewGuid4();
        locked.PartitionLockExpiresAt = now.AddMinutes(30);

        var unlocked = NewSchedule(jobDefinitionId: def);

        await Fixture.MasterRecurringSchedules.AddAsync(locked);
        await Fixture.MasterRecurringSchedules.AddAsync(unlocked);

        var queried = await Fixture.MasterRecurringSchedules.QueryAsync(new RecurringScheduleQueryCriteria { JobDefinitionId = def, CountLimit = 100 });

        Assert.Contains(queried, s => s.Id == locked.Id);
        Assert.Contains(queried, s => s.Id == unlocked.Id);
    }

    [Fact]
    public async Task Query_ShouldReturn_SchedulesWithExpiredLocks()
    {
        var def = "defQueryExpiredLock-" + JobMasterRandomUtil.NewGuid4();
        var now = DateTime.UtcNow;

        var expiredLock = NewSchedule(jobDefinitionId: def);
        expiredLock.PartitionLockId = JobMasterRandomUtil.NewGuid4();
        expiredLock.PartitionLockExpiresAt = now.AddMinutes(-10);

        await Fixture.MasterRecurringSchedules.AddAsync(expiredLock);

        var queried = await Fixture.MasterRecurringSchedules.QueryAsync(new RecurringScheduleQueryCriteria { JobDefinitionId = def, CountLimit = 100 });

        Assert.Contains(queried, s => s.Id == expiredLock.Id);
    }

    [Fact]
    public async Task Query_ShouldReturn_CanceledSchedule_AfterPartitionLockCleared()
    {
        // Regression: TryToCancel did not clear PartitionLockId/PartitionLockExpiresAt,
        // so a canceled schedule with an active lock could appear in wrong query results.
        var def = "defCanceledLock-" + JobMasterRandomUtil.NewGuid4();
        var now = DateTime.UtcNow;

        var schedule = NewSchedule(jobDefinitionId: def);
        schedule.PartitionLockId = JobMasterRandomUtil.NewGuid4();
        schedule.PartitionLockExpiresAt = now.AddMinutes(30);
        await Fixture.MasterRecurringSchedules.AddAsync(schedule);

        schedule.TryToCancel();
        await Fixture.MasterRecurringSchedules.UpdateAsync(schedule);

        var queried = await Fixture.MasterRecurringSchedules.QueryAsync(new RecurringScheduleQueryCriteria { JobDefinitionId = def, CountLimit = 100 });

        var fromDb = Assert.Single(queried, s => s.Id == schedule.Id);
        Assert.Equal(RecurringScheduleStatus.Canceled, fromDb.Status);
        Assert.Null(fromDb.PartitionLockId);
        Assert.Null(fromDb.PartitionLockExpiresAt);
    }

    // -----------------------------------------------------------------------
    // AcquireAndFetch conformance tests
    // -----------------------------------------------------------------------

    [Fact]
    public async Task AcquireAndFetch_ShouldLockAndReturnMatchingSchedules()
    {
        var def = "defAcquire-" + JobMasterRandomUtil.NewGuid4();
        var now = DateTime.UtcNow;

        var s1 = NewSchedule(jobDefinitionId: def);
        var s2 = NewSchedule(jobDefinitionId: def);
        await Fixture.MasterRecurringSchedules.AddAsync(s1);
        await Fixture.MasterRecurringSchedules.AddAsync(s2);

        var lockId = JobMasterRandomUtil.NewGuid4();
        var criteria = new RecurringScheduleQueryCriteria { JobDefinitionId = def, Status = RecurringScheduleStatus.Active, CountLimit = 100 };
        var acquired = await Fixture.MasterRecurringSchedules.AcquireAndFetchAsync(criteria, lockId, now.AddMinutes(30));

        Assert.Equal(2, acquired.Count);
        Assert.All(acquired, s =>
        {
            Assert.Equal(lockId, s.PartitionLockId);
            Assert.NotNull(s.PartitionLockExpiresAt);
        });

        var ids = acquired.Select(x => x.Id).ToHashSet();
        Assert.Contains(s1.Id, ids);
        Assert.Contains(s2.Id, ids);
    }

    [Fact]
    public async Task AcquireAndFetch_ShouldSkipAlreadyLockedSchedules()
    {
        var def = "defAcquireSkip-" + JobMasterRandomUtil.NewGuid4();
        var now = DateTime.UtcNow;

        var locked = NewSchedule(jobDefinitionId: def);
        locked.PartitionLockId = JobMasterRandomUtil.NewGuid4();
        locked.PartitionLockExpiresAt = now.AddMinutes(30);

        var unlocked = NewSchedule(jobDefinitionId: def);
        unlocked.PartitionLockId = null;
        unlocked.PartitionLockExpiresAt = null;

        await Fixture.MasterRecurringSchedules.AddAsync(locked);
        await Fixture.MasterRecurringSchedules.AddAsync(unlocked);

        var lockId = JobMasterRandomUtil.NewGuid4();
        var criteria = new RecurringScheduleQueryCriteria { JobDefinitionId = def, Status = RecurringScheduleStatus.Active, CountLimit = 100 };
        var acquired = await Fixture.MasterRecurringSchedules.AcquireAndFetchAsync(criteria, lockId, now.AddMinutes(30));

        Assert.Single(acquired);
        Assert.Equal(unlocked.Id, acquired[0].Id);
        Assert.Equal(lockId, acquired[0].PartitionLockId);
    }

    [Fact]
    public async Task AcquireAndFetch_ShouldReacquireExpiredLocks()
    {
        var def = "defAcquireExpired-" + JobMasterRandomUtil.NewGuid4();
        var now = DateTime.UtcNow;
        var oldLockId = JobMasterRandomUtil.NewGuid4();

        var expiredLock = NewSchedule(jobDefinitionId: def);
        expiredLock.PartitionLockId = oldLockId;
        expiredLock.PartitionLockExpiresAt = now.AddMinutes(-10);

        await Fixture.MasterRecurringSchedules.AddAsync(expiredLock);

        var newLockId = JobMasterRandomUtil.NewGuid4();
        var criteria = new RecurringScheduleQueryCriteria { JobDefinitionId = def, Status = RecurringScheduleStatus.Active, CountLimit = 100 };
        var acquired = await Fixture.MasterRecurringSchedules.AcquireAndFetchAsync(criteria, newLockId, now.AddMinutes(30));

        Assert.Single(acquired);
        Assert.Equal(expiredLock.Id, acquired[0].Id);
        Assert.Equal(newLockId, acquired[0].PartitionLockId);
    }

    [Fact]
    public async Task AcquireAndFetch_SecondAcquireShouldNotReturnAlreadyAcquiredSchedules()
    {
        var def = "defAcquireNoOverlap-" + JobMasterRandomUtil.NewGuid4();
        var now = DateTime.UtcNow;

        var s1 = NewSchedule(jobDefinitionId: def);
        var s2 = NewSchedule(jobDefinitionId: def);
        await Fixture.MasterRecurringSchedules.AddAsync(s1);
        await Fixture.MasterRecurringSchedules.AddAsync(s2);

        var criteria = new RecurringScheduleQueryCriteria { JobDefinitionId = def, Status = RecurringScheduleStatus.Active, CountLimit = 100 };
        var first = await Fixture.MasterRecurringSchedules.AcquireAndFetchAsync(criteria, JobMasterRandomUtil.NewGuid4(), now.AddMinutes(30));
        Assert.Equal(2, first.Count);

        var second = await Fixture.MasterRecurringSchedules.AcquireAndFetchAsync(criteria, JobMasterRandomUtil.NewGuid4(), now.AddMinutes(30));
        Assert.Empty(second);
    }

    [Fact]
    public async Task AcquireAndFetch_ShouldRespectQueryCriteriaFilters()
    {
        var def = "defAcquireFilter-" + JobMasterRandomUtil.NewGuid4();
        var now = DateTime.UtcNow;

        var match = NewSchedule(jobDefinitionId: def);
        match.Status = RecurringScheduleStatus.Active;

        var noMatch = NewSchedule(jobDefinitionId: def);
        noMatch.Status = RecurringScheduleStatus.Inactive;
        noMatch.TerminatedAt = now;

        await Fixture.MasterRecurringSchedules.AddAsync(match);
        await Fixture.MasterRecurringSchedules.AddAsync(noMatch);

        var lockId = JobMasterRandomUtil.NewGuid4();
        var criteria = new RecurringScheduleQueryCriteria { JobDefinitionId = def, Status = RecurringScheduleStatus.Active, CountLimit = 100 };
        var acquired = await Fixture.MasterRecurringSchedules.AcquireAndFetchAsync(criteria, lockId, now.AddMinutes(30));

        Assert.Single(acquired);
        Assert.Equal(match.Id, acquired[0].Id);
    }

    [Fact]
    public async Task AcquireAndFetch_ShouldBumpVersion()
    {
        var def = "defAcquireVersion-" + JobMasterRandomUtil.NewGuid4();
        var now = DateTime.UtcNow;

        var schedule = NewSchedule(jobDefinitionId: def);
        await Fixture.MasterRecurringSchedules.AddAsync(schedule);

        var beforeAcquire = await Fixture.MasterRecurringSchedules.GetAsync(schedule.Id);
        var originalVersion = beforeAcquire!.Version;

        var criteria = new RecurringScheduleQueryCriteria { JobDefinitionId = def, Status = RecurringScheduleStatus.Active, CountLimit = 100 };
        var acquired = await Fixture.MasterRecurringSchedules.AcquireAndFetchAsync(criteria, JobMasterRandomUtil.NewGuid4(), now.AddMinutes(30));

        Assert.Single(acquired);
        Assert.False(string.IsNullOrEmpty(acquired[0].Version));
        Assert.NotEqual(originalVersion, acquired[0].Version);
    }

    [Fact]
    public async Task AcquireAndFetch_ConcurrentCalls_ShouldNotDoubleAcquire_AnySchedule()
    {
        var def = "defAcquireConcurrent-" + JobMasterRandomUtil.NewGuid4();
        var now = DateTime.UtcNow;
        const int scheduleCount = 100;
        const int callers = 10;

        for (var i = 0; i < scheduleCount; i++)
        {
            var s = NewSchedule(jobDefinitionId: def);
            s.LastPlanCoverageUntil = now.AddHours(i);
            await Fixture.MasterRecurringSchedules.AddAsync(s);
        }

        var lockIds = Enumerable.Range(0, callers).Select(_ => JobMasterRandomUtil.NewGuid4()).ToArray();
        var criteria = new RecurringScheduleQueryCriteria { JobDefinitionId = def, Status = RecurringScheduleStatus.Active, CountLimit = scheduleCount };

        var tasks = lockIds.Select(id => Fixture.MasterRecurringSchedules.AcquireAndFetchAsync(criteria, id, now.AddMinutes(30))).ToArray();
        var results = await Task.WhenAll(tasks);

        var allAcquiredIds = results.SelectMany(r => r.Select(s => s.Id)).ToList();

        // No schedule must appear in more than one caller's result
        Assert.Equal(allAcquiredIds.Count, allAcquiredIds.Distinct().Count());

        // Together all callers must have claimed every schedule exactly once
        Assert.Equal(scheduleCount, allAcquiredIds.Count);

        // Each returned schedule must carry the lockId of the caller that acquired it
        for (var c = 0; c < callers; c++)
        {
            var expectedLockId = lockIds[c];
            Assert.All(results[c], s => Assert.Equal(expectedLockId, s.PartitionLockId));
        }
    }

    [Fact]
    public async Task AcquireAndFetch_ShouldRespectCountLimit()
    {
        var def = "defAcquireLimit-" + JobMasterRandomUtil.NewGuid4();
        var now = DateTime.UtcNow;
        var lockId1 = JobMasterRandomUtil.NewGuid4();
        var lockId2 = JobMasterRandomUtil.NewGuid4();

        for (var i = 0; i < 5; i++)
        {
            var s = NewSchedule(jobDefinitionId: def);
            s.LastPlanCoverageUntil = now.AddHours(i);
            await Fixture.MasterRecurringSchedules.AddAsync(s);
        }

        var criteria = new RecurringScheduleQueryCriteria { JobDefinitionId = def, Status = RecurringScheduleStatus.Active, CountLimit = 2 };
        var first = await Fixture.MasterRecurringSchedules.AcquireAndFetchAsync(criteria, lockId1, now.AddMinutes(30));

        Assert.Equal(2, first.Count);
        Assert.All(first, s => Assert.Equal(lockId1, s.PartitionLockId));

        var second = await Fixture.MasterRecurringSchedules.AcquireAndFetchAsync(criteria, lockId2, now.AddMinutes(30));
        Assert.Equal(2, second.Count);
        Assert.All(second, s => Assert.Equal(lockId2, s.PartitionLockId));

        var firstIds = first.Select(x => x.Id).ToHashSet();
        var secondIds = second.Select(x => x.Id).ToHashSet();
        Assert.Empty(firstIds.Intersect(secondIds));
    }

    // -----------------------------------------------------------------------
    // Query criteria conformance: one shared dataset, one Theory per criteria
    // -----------------------------------------------------------------------

    private sealed record QueryDataset(
        string Def,
        RecurringScheduleRawModel ActiveDynLaneA,
        RecurringScheduleRawModel ActiveStatLaneB,
        RecurringScheduleRawModel CanceledLaneA,
        RecurringScheduleRawModel InactiveLaneB,
        RecurringScheduleRawModel PendingSave,
        RecurringScheduleRawModel ActiveLock,
        RecurringScheduleRawModel ExpiredLock,
        RecurringScheduleRawModel CoverageNear,
        RecurringScheduleRawModel CoverageFar,
        RecurringScheduleRawModel StartAfterPast,
        RecurringScheduleRawModel StartAfterFuture,
        RecurringScheduleRawModel CancelPending);

    private async Task<QueryDataset> CreateQueryDatasetAsync()
    {
        var def = "defQ-" + JobMasterRandomUtil.NewGuid4();
        var now = DateTime.UtcNow;

        var activeDynLaneA = NewSchedule(def);
        activeDynLaneA.Status = RecurringScheduleStatus.Active;
        activeDynLaneA.RecurringScheduleType = RecurringScheduleType.Dynamic;
        activeDynLaneA.WorkerLane = "LANE_A";
        activeDynLaneA.ProfileId = "p1";
        activeDynLaneA.LastPlanCoverageUntil = now.AddHours(3);

        var activeStatLaneB = NewSchedule(def);
        activeStatLaneB.Status = RecurringScheduleStatus.Active;
        activeStatLaneB.RecurringScheduleType = RecurringScheduleType.Static;
        activeStatLaneB.WorkerLane = "LANE_B";
        activeStatLaneB.ProfileId = "p2";
        activeStatLaneB.LastPlanCoverageUntil = now.AddHours(3);

        var canceledLaneA = NewSchedule(def);
        canceledLaneA.Status = RecurringScheduleStatus.Canceled;
        canceledLaneA.WorkerLane = "LANE_A";
        canceledLaneA.TerminatedAt = now;
        canceledLaneA.IsJobCancellationPending = true;
        canceledLaneA.LastPlanCoverageUntil = now.AddHours(1);

        var inactiveLaneB = NewSchedule(def);
        inactiveLaneB.Status = RecurringScheduleStatus.Inactive;
        inactiveLaneB.WorkerLane = "LANE_B";
        inactiveLaneB.TerminatedAt = now;
        inactiveLaneB.LastPlanCoverageUntil = now.AddHours(1);

        var pendingSave = NewSchedule(def);
        pendingSave.Status = RecurringScheduleStatus.PendingSave;
        pendingSave.WorkerLane = "LANE_A";
        pendingSave.LastPlanCoverageUntil = now.AddHours(1);

        var activeLock = NewSchedule(def);
        activeLock.PartitionLockId = JobMasterRandomUtil.NewGuid4();
        activeLock.PartitionLockExpiresAt = now.AddMinutes(30);
        activeLock.LastPlanCoverageUntil = now.AddHours(3);

        var expiredLock = NewSchedule(def);
        expiredLock.PartitionLockId = JobMasterRandomUtil.NewGuid4();
        expiredLock.PartitionLockExpiresAt = now.AddMinutes(-10);
        expiredLock.LastPlanCoverageUntil = now.AddHours(3);

        var coverageNear = NewSchedule(def);
        coverageNear.LastPlanCoverageUntil = now.AddHours(1);

        var coverageFar = NewSchedule(def);
        coverageFar.LastPlanCoverageUntil = now.AddHours(5);

        var startAfterPast = NewSchedule(def);
        startAfterPast.StartAfter = now.AddHours(-2);
        startAfterPast.LastPlanCoverageUntil = now.AddHours(3);

        var startAfterFuture = NewSchedule(def);
        startAfterFuture.StartAfter = now.AddHours(10);
        startAfterFuture.LastPlanCoverageUntil = now.AddHours(3);

        var cancelPending = NewSchedule(def);
        cancelPending.IsJobCancellationPending = true;
        cancelPending.LastPlanCoverageUntil = now.AddHours(3);

        foreach (var s in new[] { activeDynLaneA, activeStatLaneB, canceledLaneA, inactiveLaneB, pendingSave, activeLock, expiredLock, coverageNear, coverageFar, startAfterPast, startAfterFuture, cancelPending })
            await Fixture.MasterRecurringSchedules.AddAsync(s);

        return new QueryDataset(def, activeDynLaneA, activeStatLaneB, canceledLaneA, inactiveLaneB, pendingSave, activeLock, expiredLock, coverageNear, coverageFar, startAfterPast, startAfterFuture, cancelPending);
    }

    [Theory]
    [InlineData("NoFilter")]
    [InlineData("Status_Active")]
    [InlineData("Status_Canceled")]
    [InlineData("Status_Inactive")]
    [InlineData("Status_PendingSave")]
    [InlineData("WorkerLane_A")]
    [InlineData("WorkerLane_B")]
    [InlineData("RecurringScheduleType_Static")]
    [InlineData("RecurringScheduleType_Dynamic")]
    [InlineData("ProfileId")]
    [InlineData("CoverageUntil")]
    [InlineData("IsJobCancellationPending")]
    [InlineData("CanceledOrInactive")]
    [InlineData("StartAfterTo")]
    [InlineData("StartAfterFrom")]
    public async Task Query_AllCriteria(string testCase)
    {
        var d = await CreateQueryDatasetAsync();
        var now = DateTime.UtcNow;

        var (criteria, mustContain, mustNotContain) = testCase switch
        {
            // Regression: QueryAsync must return locked schedules — no implicit isLocked filter
            "NoFilter" => (
                new RecurringScheduleQueryCriteria { JobDefinitionId = d.Def, CountLimit = 100 },
                new[] { d.ActiveLock.Id, d.ExpiredLock.Id, d.ActiveDynLaneA.Id },
                Array.Empty<Guid>()),

            "Status_Active" => (
                new RecurringScheduleQueryCriteria { JobDefinitionId = d.Def, Status = RecurringScheduleStatus.Active, CountLimit = 100 },
                new[] { d.ActiveDynLaneA.Id, d.ActiveStatLaneB.Id, d.ActiveLock.Id, d.ExpiredLock.Id },
                new[] { d.CanceledLaneA.Id, d.InactiveLaneB.Id, d.PendingSave.Id }),

            "Status_Canceled" => (
                new RecurringScheduleQueryCriteria { JobDefinitionId = d.Def, Status = RecurringScheduleStatus.Canceled, CountLimit = 100 },
                new[] { d.CanceledLaneA.Id },
                new[] { d.ActiveDynLaneA.Id, d.InactiveLaneB.Id }),

            "Status_Inactive" => (
                new RecurringScheduleQueryCriteria { JobDefinitionId = d.Def, Status = RecurringScheduleStatus.Inactive, CountLimit = 100 },
                new[] { d.InactiveLaneB.Id },
                new[] { d.ActiveDynLaneA.Id, d.CanceledLaneA.Id }),

            "Status_PendingSave" => (
                new RecurringScheduleQueryCriteria { JobDefinitionId = d.Def, Status = RecurringScheduleStatus.PendingSave, CountLimit = 100 },
                new[] { d.PendingSave.Id },
                new[] { d.ActiveDynLaneA.Id, d.CanceledLaneA.Id }),

            "WorkerLane_A" => (
                new RecurringScheduleQueryCriteria { JobDefinitionId = d.Def, WorkerLane = "LANE_A", CountLimit = 100 },
                new[] { d.ActiveDynLaneA.Id, d.CanceledLaneA.Id, d.PendingSave.Id },
                new[] { d.ActiveStatLaneB.Id, d.InactiveLaneB.Id }),

            "WorkerLane_B" => (
                new RecurringScheduleQueryCriteria { JobDefinitionId = d.Def, WorkerLane = "LANE_B", CountLimit = 100 },
                new[] { d.ActiveStatLaneB.Id, d.InactiveLaneB.Id },
                new[] { d.ActiveDynLaneA.Id, d.CanceledLaneA.Id }),

            "RecurringScheduleType_Static" => (
                new RecurringScheduleQueryCriteria { JobDefinitionId = d.Def, RecurringScheduleType = RecurringScheduleType.Static, CountLimit = 100 },
                new[] { d.ActiveStatLaneB.Id },
                new[] { d.ActiveDynLaneA.Id }),

            "RecurringScheduleType_Dynamic" => (
                new RecurringScheduleQueryCriteria { JobDefinitionId = d.Def, RecurringScheduleType = RecurringScheduleType.Dynamic, CountLimit = 100 },
                new[] { d.ActiveDynLaneA.Id },
                new[] { d.ActiveStatLaneB.Id }),

            "ProfileId" => (
                new RecurringScheduleQueryCriteria { JobDefinitionId = d.Def, ProfileId = "p1", CountLimit = 100 },
                new[] { d.ActiveDynLaneA.Id },
                new[] { d.ActiveStatLaneB.Id }),

            "CoverageUntil" => (
                new RecurringScheduleQueryCriteria { JobDefinitionId = d.Def, CoverageUntil = now.AddHours(2), CountLimit = 100 },
                new[] { d.CoverageNear.Id },
                new[] { d.CoverageFar.Id }),

            "IsJobCancellationPending" => (
                new RecurringScheduleQueryCriteria { JobDefinitionId = d.Def, IsJobCancellationPending = true, CountLimit = 100 },
                new[] { d.CanceledLaneA.Id, d.CancelPending.Id },
                new[] { d.ActiveDynLaneA.Id, d.InactiveLaneB.Id }),

            "CanceledOrInactive" => (
                new RecurringScheduleQueryCriteria { JobDefinitionId = d.Def, CanceledOrInactive = true, CountLimit = 100 },
                new[] { d.CanceledLaneA.Id, d.InactiveLaneB.Id },
                new[] { d.ActiveDynLaneA.Id, d.ActiveStatLaneB.Id }),

            "StartAfterTo" => (
                new RecurringScheduleQueryCriteria { JobDefinitionId = d.Def, StartAfterTo = now.AddHours(1), CountLimit = 100 },
                new[] { d.StartAfterPast.Id },
                new[] { d.StartAfterFuture.Id }),

            "StartAfterFrom" => (
                new RecurringScheduleQueryCriteria { JobDefinitionId = d.Def, StartAfterFrom = now.AddHours(5), CountLimit = 100 },
                new[] { d.StartAfterFuture.Id },
                new[] { d.StartAfterPast.Id }),

            _ => throw new ArgumentException($"Unknown test case: {testCase}")
        };

        var results = await Fixture.MasterRecurringSchedules.QueryAsync(criteria);
        var ids = results.Select(s => s.Id).ToHashSet();

        foreach (var id in mustContain)
            Assert.Contains(id, ids);
        foreach (var id in mustNotContain)
            Assert.DoesNotContain(id, ids);
    }
}
