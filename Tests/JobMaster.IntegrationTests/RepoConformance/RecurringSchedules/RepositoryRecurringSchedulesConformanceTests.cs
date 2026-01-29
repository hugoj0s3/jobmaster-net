using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using System.Text.Json;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Serialization;
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

        var schedule = NewSchedule(jobDefinitionId: "def-rt-" + Guid.NewGuid());
        schedule.ExpressionTypeId = NeverRecursExprCompiler.TypeId;
        schedule.Expression = string.Empty;

        schedule.StaticDefinitionId = "static-" + Guid.NewGuid();
        schedule.ProfileId = "profile-" + Guid.NewGuid();

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

        schedule.PartitionLockId = 123;
        schedule.PartitionLockExpiresAt = now.AddMinutes(30);

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
    public async Task Update_ShouldPersistChanges()
    {
        var schedule = NewSchedule(jobDefinitionId: "def-upd-" + Guid.NewGuid());
        await Fixture.MasterRecurringSchedules.AddAsync(schedule);

        var updated = Clone(schedule);
        updated.JobDefinitionId = schedule.JobDefinitionId + "-updated";
        updated.ProfileId = "profile-updated";
        updated.Status = RecurringScheduleStatus.Canceled;
        updated.RecurringScheduleType = RecurringScheduleType.Dynamic;
        updated.TerminatedAt = DateTime.UtcNow;
        updated.MsgData = "{\"y\":2}";
        updated.Metadata = "{\"color\":\"red\"}";
        updated.Priority = JobMasterPriority.Low;
        updated.MaxNumberOfRetries = 2;
        updated.Timeout = TimeSpan.FromSeconds(9);
        updated.BucketId = "bucket-upd";
        updated.AgentConnectionId = Fixture.AgentConnectionId;
        updated.AgentWorkerId = "worker-upd";
        updated.PartitionLockId = 55;
        updated.PartitionLockExpiresAt = DateTime.UtcNow.AddMinutes(10);
        updated.StartAfter = DateTime.UtcNow.AddHours(-1);
        updated.EndBefore = DateTime.UtcNow.AddHours(5);
        updated.LastPlanCoverageUntil = DateTime.UtcNow.AddHours(3);
        updated.LastExecutedPlan = DateTime.UtcNow.AddMinutes(-30);
        updated.HasFailedOnLastPlanExecution = false;
        updated.IsJobCancellationPending = false;
        updated.StaticDefinitionLastEnsured = DateTime.UtcNow.AddMinutes(-1);
        updated.WorkerLane = "LANE_UPD";

        await Fixture.MasterRecurringSchedules.UpdateAsync(updated);

        var fromDb = await Fixture.MasterRecurringSchedules.GetAsync(schedule.Id);
        Assert.NotNull(fromDb);
        AssertScheduleEquivalent(updated, fromDb!);
    }

    [Fact]
    public async Task GetByStaticId_ShouldReturnOnly_StaticSchedules()
    {
        var staticId = "static-" + Guid.NewGuid();

        var matching = NewSchedule(jobDefinitionId: "def-static-" + Guid.NewGuid());
        matching.StaticDefinitionId = staticId;
        matching.RecurringScheduleType = RecurringScheduleType.Static;

        var nonStatic = NewSchedule(jobDefinitionId: "def-nonstatic-" + Guid.NewGuid());
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
        var defA = "defA-" + Guid.NewGuid();
        var defB = "defB-" + Guid.NewGuid();

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
    public async Task Query_ShouldSupport_IsLocked_And_PartitionLockId()
    {
        var def = "defLock-" + Guid.NewGuid();
        var now = DateTime.UtcNow;

        var locked = NewSchedule(jobDefinitionId: def);
        locked.PartitionLockId = 1;
        locked.PartitionLockExpiresAt = now.AddMinutes(30);

        var expired = NewSchedule(jobDefinitionId: def);
        expired.PartitionLockId = 2;
        expired.PartitionLockExpiresAt = now.AddMinutes(-30);

        var unlocked = NewSchedule(jobDefinitionId: def);
        unlocked.PartitionLockId = null;
        unlocked.PartitionLockExpiresAt = null;

        await Fixture.MasterRecurringSchedules.AddAsync(locked);
        await Fixture.MasterRecurringSchedules.AddAsync(expired);
        await Fixture.MasterRecurringSchedules.AddAsync(unlocked);

        var cLocked = new RecurringScheduleQueryCriteria { JobDefinitionId = def, IsLocked = true, CountLimit = 100 };
        var qLocked = await Fixture.MasterRecurringSchedules.QueryAsync(cLocked);
        Assert.Contains(qLocked, x => x.Id == locked.Id);
        Assert.DoesNotContain(qLocked, x => x.Id == expired.Id);
        Assert.DoesNotContain(qLocked, x => x.Id == unlocked.Id);

        var cUnlocked = new RecurringScheduleQueryCriteria { JobDefinitionId = def, IsLocked = false, CountLimit = 100 };
        var qUnlocked = await Fixture.MasterRecurringSchedules.QueryAsync(cUnlocked);
        Assert.Contains(qUnlocked, x => x.Id == expired.Id);
        Assert.Contains(qUnlocked, x => x.Id == unlocked.Id);
        Assert.DoesNotContain(qUnlocked, x => x.Id == locked.Id);

        var cLockId = new RecurringScheduleQueryCriteria { JobDefinitionId = def, PartitionLockId = 2, CountLimit = 100 };
        var qLockId = await Fixture.MasterRecurringSchedules.QueryAsync(cLockId);
        Assert.Contains(qLockId, x => x.Id == expired.Id);
        Assert.DoesNotContain(qLockId, x => x.Id == locked.Id);
    }

    [Fact]
    public async Task Query_ShouldSupport_StartAfter_And_EndBefore_Ranges_WithNulls()
    {
        var def = "defRanges-" + Guid.NewGuid();
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
        var def = "defCoverage-" + Guid.NewGuid();
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
        var def = "defCancel-" + Guid.NewGuid();

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
        var def = "defCanceledOrInactive-" + Guid.NewGuid();

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
        var def = "defTypeProfileLane-" + Guid.NewGuid();

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
        var def = "defMeta-" + Guid.NewGuid();

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
        var def = "defPaging-" + Guid.NewGuid();
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

    internal virtual RecurringScheduleRawModel NewSchedule(string? jobDefinitionId = null)
    {
        var now = DateTime.UtcNow;
        return new RecurringScheduleRawModel(Fixture.ClusterId)
        {
            Id = Guid.NewGuid(),
            ExpressionTypeId = NeverRecursExprCompiler.TypeId,
            Expression = string.Empty,
            JobDefinitionId = jobDefinitionId ?? ("def-" + Guid.NewGuid()),
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
        var def = "defPurge-" + Guid.NewGuid();
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
        var def = "defPurgeLimit-" + Guid.NewGuid();
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
        var def = "defPurgeActive-" + Guid.NewGuid();
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
}
