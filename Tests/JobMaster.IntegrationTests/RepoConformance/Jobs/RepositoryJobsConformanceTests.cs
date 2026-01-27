using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using System.Text.Json;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.Jobs;

public abstract class RepositoryJobsConformanceTests<TFixture>
    where TFixture : RepositoryFixtureBase
{
    protected TFixture Fixture { get; }

    protected RepositoryJobsConformanceTests(TFixture fixture)
    {
        Fixture = fixture;
    }

    [Fact]
    public async Task AddAndGet_ShouldRoundTrip()
    {
        var now = DateTime.UtcNow;

        var job = NewJob(
            jobDefinitionId: "job-def-rt-" + Guid.NewGuid(),
            status: JobMasterJobStatus.AssignedToBucket,
            scheduledAt: now.AddMinutes(1),
            workerLane: "LANE_RT");

        job.BucketId = "bucket-1";
        job.AgentConnectionId = Fixture.AgentConnectionId;
        job.AgentWorkerId = "worker-1";
        job.NumberOfFailures = 2;
        job.MaxNumberOfRetries = 7;
        job.Timeout = TimeSpan.FromSeconds(123);
        job.RecurringScheduleId = Guid.NewGuid();
        job.PartitionLockId = 42;
        job.PartitionLockExpiresAt = now.AddMinutes(30);
        job.ProcessDeadline = now.AddMinutes(20);
        job.ProcessingStartedAt = now.AddMinutes(-2);
        job.SucceedExecutedAt = now.AddMinutes(-1);
        job.MsgData = "{\"a\":1,\"b\":\"x\"}";
        job.Metadata = "{\"meta_k\":\"meta_v\",\"n\":5}";

        await Fixture.MasterJobs.AddAsync(job);

        var fromDb = await Fixture.MasterJobs.GetAsync(job.Id);

        Assert.NotNull(fromDb);
        AssertJobEquivalent(job, fromDb!);
        // Version should be initialized on insert
        Assert.False(string.IsNullOrEmpty(fromDb!.Version));
    }

    [Fact]
    public async Task Update_ShouldPersistChanges()
    {
        var job = NewJob();
        await Fixture.MasterJobs.AddAsync(job);

        var updated = Clone(job);
        var originalVersion = job.Version;
        updated.JobDefinitionId = job.JobDefinitionId + "-updated";
        updated.Status = JobMasterJobStatus.Succeeded;
        updated.ScheduledAt = job.ScheduledAt.AddMinutes(5);
        updated.WorkerLane = "LANE_UPDATE";
        updated.BucketId = "bucket-updated";
        updated.AgentConnectionId = Fixture.AgentConnectionId;
        updated.AgentWorkerId = "worker-updated";
        updated.NumberOfFailures = 3;
        updated.MaxNumberOfRetries = 9;
        updated.Timeout = TimeSpan.FromSeconds(77);
        updated.RecurringScheduleId = Guid.NewGuid();
        updated.PartitionLockId = 11;
        updated.PartitionLockExpiresAt = DateTime.UtcNow.AddMinutes(15);
        updated.ProcessDeadline = DateTime.UtcNow.AddMinutes(5);
        updated.ProcessingStartedAt = DateTime.UtcNow.AddMinutes(-10);
        updated.SucceedExecutedAt = DateTime.UtcNow.AddMinutes(-1);
        updated.MsgData = "{\"x\":\"y\"}";
        updated.Metadata = "{\"k\":\"v\"}";

        await Fixture.MasterJobs.UpdateAsync(updated);

        var fromDb = await Fixture.MasterJobs.GetAsync(job.Id);

        Assert.NotNull(fromDb);
        AssertJobEquivalent(updated, fromDb!);
        // Version should change on update
        Assert.False(string.IsNullOrEmpty(fromDb!.Version));
        Assert.NotEqual(originalVersion, fromDb!.Version);
    }

    [Fact]
    public async Task Update_ShouldThrow_OnVersionConflict()
    {
        var job = NewJob();
        await Fixture.MasterJobs.AddAsync(job);

        // Load two separate copies to simulate concurrent updates
        var copyA = await Fixture.MasterJobs.GetAsync(job.Id);
        var copyB = await Fixture.MasterJobs.GetAsync(job.Id);
        Assert.NotNull(copyA);
        Assert.NotNull(copyB);

        // First update succeeds and advances the version
        copyA!.JobDefinitionId = copyA.JobDefinitionId + "-A";
        await Fixture.MasterJobs.UpdateAsync(copyA);

        // Second update uses stale version and must fail
        copyB!.JobDefinitionId = copyB.JobDefinitionId + "-B";
        await Assert.ThrowsAsync<JobMasterVersionConflictException>(async () =>
        {
            await Fixture.MasterJobs.UpdateAsync(copyB);
        });
    }

    [Fact]
    public async Task Update_ShouldThrow_WhenVersionMismatch_Manual()
    {
        var job = NewJob();
        await Fixture.MasterJobs.AddAsync(job);

        // Get latest from DB to ensure we have a real current version
        var current = await Fixture.MasterJobs.GetAsync(job.Id);
        Assert.NotNull(current);
        Assert.False(string.IsNullOrEmpty(current!.Version));

        // Clone and force an incorrect (random) version
        var wrong = Clone(current);
        wrong.Version = Guid.NewGuid().ToString("N"); // wrong expected version
        wrong.JobDefinitionId = wrong.JobDefinitionId + "-WRONG-VERSION";

        await Assert.ThrowsAsync<JobMasterVersionConflictException>(async () =>
        {
            await Fixture.MasterJobs.UpdateAsync(wrong);
        });
    }

    [Fact]
    public async Task Query_And_Count_And_QueryIds_ShouldBeConsistent()
    {
        var baseTime = DateTime.UtcNow;

        var defA = "defA-" + Guid.NewGuid();
        var defB = "defB-" + Guid.NewGuid();

        var j1 = NewJob(jobDefinitionId: defA, status: JobMasterJobStatus.HeldOnMaster, scheduledAt: baseTime.AddMinutes(1), workerLane: "LANE_1");
        var j2 = NewJob(jobDefinitionId: defA, status: JobMasterJobStatus.HeldOnMaster, scheduledAt: baseTime.AddMinutes(2), workerLane: "LANE_2");
        var j3 = NewJob(jobDefinitionId: defB, status: JobMasterJobStatus.Succeeded, scheduledAt: baseTime.AddMinutes(3), workerLane: "LANE_1");

        await Fixture.MasterJobs.AddAsync(j1);
        await Fixture.MasterJobs.AddAsync(j2);
        await Fixture.MasterJobs.AddAsync(j3);

        var criteria = new JobQueryCriteria
        {
            Status = JobMasterJobStatus.HeldOnMaster,
            JobDefinitionId = defA,
            ScheduledFrom = baseTime.AddSeconds(30),
            ScheduledTo = baseTime.AddMinutes(2).AddSeconds(30),
            WorkerLane = null,
            CountLimit = 100,
            Offset = 0
        };

        var queried = await Fixture.MasterJobs.QueryAsync(criteria);
        var count = Fixture.MasterJobs.Count(criteria);
        var ids = await Fixture.MasterJobs.QueryIdsAsync(criteria);

        Assert.Equal(count, queried.Count);
        Assert.Equal(queried.Count, ids.Count);

        var queriedIds = queried.Select(x => x.Id).OrderBy(x => x).ToList();
        var idsSorted = ids.OrderBy(x => x).ToList();

        Assert.Equal(queriedIds, idsSorted);
    }

    [Fact]
    public async Task Query_ShouldSupport_Status_Filter()
    {
        var def = "defStatus-" + Guid.NewGuid();
        await Fixture.MasterJobs.AddAsync(NewJob(jobDefinitionId: def, status: JobMasterJobStatus.HeldOnMaster));
        await Fixture.MasterJobs.AddAsync(NewJob(jobDefinitionId: def, status: JobMasterJobStatus.Succeeded));

        var c = new JobQueryCriteria { JobDefinitionId = def, Status = JobMasterJobStatus.Succeeded, CountLimit = 100 };
        var queried = await Fixture.MasterJobs.QueryAsync(c);

        Assert.NotEmpty(queried);
        Assert.All(queried, j => Assert.Equal(JobMasterJobStatus.Succeeded, j.Status));
    }

    [Fact]
    public async Task Query_ShouldSupport_ScheduledFrom_Filter()
    {
        var def = "defScheduledFrom-" + Guid.NewGuid();
        var baseTime = DateTime.UtcNow;

        var early = NewJob(jobDefinitionId: def, scheduledAt: baseTime.AddMinutes(1));
        var late = NewJob(jobDefinitionId: def, scheduledAt: baseTime.AddMinutes(10));
        await Fixture.MasterJobs.AddAsync(early);
        await Fixture.MasterJobs.AddAsync(late);

        var c = new JobQueryCriteria { JobDefinitionId = def, ScheduledFrom = baseTime.AddMinutes(5), CountLimit = 100 };
        var queried = await Fixture.MasterJobs.QueryAsync(c);

        Assert.Contains(queried, j => j.Id == late.Id);
        Assert.DoesNotContain(queried, j => j.Id == early.Id);
    }

    [Fact]
    public async Task Query_ShouldSupport_ScheduledTo_Filter()
    {
        var def = "defScheduledTo-" + Guid.NewGuid();
        var baseTime = DateTime.UtcNow;

        var early = NewJob(jobDefinitionId: def, scheduledAt: baseTime.AddMinutes(1));
        var late = NewJob(jobDefinitionId: def, scheduledAt: baseTime.AddMinutes(10));
        await Fixture.MasterJobs.AddAsync(early);
        await Fixture.MasterJobs.AddAsync(late);

        var c = new JobQueryCriteria { JobDefinitionId = def, ScheduledTo = baseTime.AddMinutes(5), CountLimit = 100 };
        var queried = await Fixture.MasterJobs.QueryAsync(c);

        Assert.Contains(queried, j => j.Id == early.Id);
        Assert.DoesNotContain(queried, j => j.Id == late.Id);
    }

    [Fact]
    public async Task Query_ShouldSupport_ProcessDeadlineTo_Filter()
    {
        var def = "defDeadline-" + Guid.NewGuid();
        var now = DateTime.UtcNow;

        var within = NewJob(jobDefinitionId: def);
        within.ProcessDeadline = now.AddMinutes(5);
        var after = NewJob(jobDefinitionId: def);
        after.ProcessDeadline = now.AddMinutes(50);

        await Fixture.MasterJobs.AddAsync(within);
        await Fixture.MasterJobs.AddAsync(after);

        var c = new JobQueryCriteria { JobDefinitionId = def, ProcessDeadlineTo = now.AddMinutes(10), CountLimit = 100 };
        var queried = await Fixture.MasterJobs.QueryAsync(c);

        Assert.Contains(queried, j => j.Id == within.Id);
        Assert.DoesNotContain(queried, j => j.Id == after.Id);
    }

    [Fact]
    public async Task Query_ShouldSupport_RecurringScheduleId_Filter()
    {
        var def = "defRecurring-" + Guid.NewGuid();
        var recurringId = Guid.NewGuid();

        var match = NewJob(jobDefinitionId: def);
        match.RecurringScheduleId = recurringId;
        var other = NewJob(jobDefinitionId: def);
        other.RecurringScheduleId = Guid.NewGuid();

        await Fixture.MasterJobs.AddAsync(match);
        await Fixture.MasterJobs.AddAsync(other);

        var c = new JobQueryCriteria { JobDefinitionId = def, RecurringScheduleId = recurringId, CountLimit = 100 };
        var queried = await Fixture.MasterJobs.QueryAsync(c);

        Assert.Contains(queried, j => j.Id == match.Id);
        Assert.DoesNotContain(queried, j => j.Id == other.Id);
    }

    [Fact]
    public async Task Query_ShouldSupport_JobDefinitionId_Filter()
    {
        var defA = "defA-" + Guid.NewGuid();
        var defB = "defB-" + Guid.NewGuid();
        var a = NewJob(jobDefinitionId: defA);
        var b = NewJob(jobDefinitionId: defB);
        await Fixture.MasterJobs.AddAsync(a);
        await Fixture.MasterJobs.AddAsync(b);

        var c = new JobQueryCriteria { JobDefinitionId = defA, CountLimit = 100 };
        var queried = await Fixture.MasterJobs.QueryAsync(c);

        Assert.Contains(queried, j => j.Id == a.Id);
        Assert.DoesNotContain(queried, j => j.Id == b.Id);
    }

    [Fact]
    public async Task Query_ShouldSupport_IsLocked_Filter_True_And_False()
    {
        var def = "defLocked-" + Guid.NewGuid();
        var now = DateTime.UtcNow;

        var locked = NewJob(jobDefinitionId: def);
        locked.PartitionLockId = 1;
        locked.PartitionLockExpiresAt = now.AddMinutes(30);

        var unlocked = NewJob(jobDefinitionId: def);
        unlocked.PartitionLockId = null;
        unlocked.PartitionLockExpiresAt = null;

        var expired = NewJob(jobDefinitionId: def);
        expired.PartitionLockId = 2;
        expired.PartitionLockExpiresAt = now.AddMinutes(-30);

        await Fixture.MasterJobs.AddAsync(locked);
        await Fixture.MasterJobs.AddAsync(unlocked);
        await Fixture.MasterJobs.AddAsync(expired);

        var cLocked = new JobQueryCriteria { JobDefinitionId = def, IsLocked = true, CountLimit = 100 };
        var lockedResult = await Fixture.MasterJobs.QueryAsync(cLocked);
        Assert.Contains(lockedResult, j => j.Id == locked.Id);
        Assert.DoesNotContain(lockedResult, j => j.Id == unlocked.Id);
        Assert.DoesNotContain(lockedResult, j => j.Id == expired.Id);

        var cUnlocked = new JobQueryCriteria { JobDefinitionId = def, IsLocked = false, CountLimit = 100 };
        var unlockedResult = await Fixture.MasterJobs.QueryAsync(cUnlocked);
        Assert.Contains(unlockedResult, j => j.Id == unlocked.Id);
        Assert.Contains(unlockedResult, j => j.Id == expired.Id);
        Assert.DoesNotContain(unlockedResult, j => j.Id == locked.Id);
    }

    [Fact]
    public async Task Query_ShouldSupport_PartitionLockId_Filter()
    {
        var def = "defLockId-" + Guid.NewGuid();
        var a = NewJob(jobDefinitionId: def);
        a.PartitionLockId = 10;
        var b = NewJob(jobDefinitionId: def);
        b.PartitionLockId = 11;

        await Fixture.MasterJobs.AddAsync(a);
        await Fixture.MasterJobs.AddAsync(b);

        var c = new JobQueryCriteria { JobDefinitionId = def, PartitionLockId = 11, CountLimit = 100 };
        var queried = await Fixture.MasterJobs.QueryAsync(c);

        Assert.Contains(queried, j => j.Id == b.Id);
        Assert.DoesNotContain(queried, j => j.Id == a.Id);
    }

    [Fact]
    public async Task Query_ShouldSupport_WorkerLane_Filter()
    {
        var def = "defLane-" + Guid.NewGuid();
        var baseTime = DateTime.UtcNow;

        var lane1 = NewJob(jobDefinitionId: def, status: JobMasterJobStatus.HeldOnMaster, scheduledAt: baseTime.AddMinutes(1), workerLane: "LANE_A");
        var lane2 = NewJob(jobDefinitionId: def, status: JobMasterJobStatus.HeldOnMaster, scheduledAt: baseTime.AddMinutes(2), workerLane: "LANE_B");

        await Fixture.MasterJobs.AddAsync(lane1);
        await Fixture.MasterJobs.AddAsync(lane2);

        var criteria = new JobQueryCriteria
        {
            JobDefinitionId = def,
            WorkerLane = "LANE_A",
            CountLimit = 100,
            Offset = 0
        };

        var queried = await Fixture.MasterJobs.QueryAsync(criteria);

        Assert.All(queried, j => Assert.Equal("LANE_A", j.WorkerLane));
        Assert.Contains(queried, j => j.Id == lane1.Id);
        Assert.DoesNotContain(queried, j => j.Id == lane2.Id);
    }

    [Fact]
    public async Task Query_ShouldSupport_CountLimit_And_Offset()
    {
        var def = "defPaging-" + Guid.NewGuid();
        var baseTime = DateTime.UtcNow;

        var jobs = new List<JobRawModel>();
        for (var i = 0; i < 5; i++)
        {
            var j = NewJob(jobDefinitionId: def, scheduledAt: baseTime.AddMinutes(i));
            jobs.Add(j);
            await Fixture.MasterJobs.AddAsync(j);
        }

        var c = new JobQueryCriteria
        {
            JobDefinitionId = def,
            CountLimit = 2,
            Offset = 1
        };

        var queried = await Fixture.MasterJobs.QueryAsync(c);
        Assert.Equal(2, queried.Count);

        var ordered = jobs.OrderBy(x => x.ScheduledAt).ThenBy(x => x.CreatedAt).Select(x => x.Id).ToList();
        var expected = ordered.Skip(1).Take(2).ToList();
        Assert.Equal(expected, queried.Select(x => x.Id).ToList());
    }

    [Fact]
    public async Task Query_ShouldSupport_MetadataFilters_AllOperations_And_Types()
    {
        var def = "defMeta-" + Guid.NewGuid();

        var t0 = new DateTime(2025, 01, 01, 0, 0, 0, DateTimeKind.Utc);

        var a = NewJob(jobDefinitionId: def);
        a.Metadata = "{\"s\":\"alpha\",\"n\":10,\"dt\":\"2025-01-01T00:00:00Z\"}";

        var b = NewJob(jobDefinitionId: def);
        b.Metadata = "{\"s\":\"alphabet\",\"n\":20,\"dt\":\"2025-01-02T00:00:00Z\"}";

        var cJob = NewJob(jobDefinitionId: def);
        cJob.Metadata = "{\"s\":\"beta\",\"n\":30,\"dt\":\"2025-01-03T00:00:00Z\"}";

        await Fixture.MasterJobs.AddAsync(a);
        await Fixture.MasterJobs.AddAsync(b);
        await Fixture.MasterJobs.AddAsync(cJob);

        // String operations
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "s", Operation = GenericFilterOperation.Eq, Value = "alpha" }, a.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "s", Operation = GenericFilterOperation.Neq, Value = "alpha" }, b.Id, cJob.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "s", Operation = GenericFilterOperation.In, Values = new object?[] { "alpha", "beta" } }, a.Id, cJob.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "s", Operation = GenericFilterOperation.Contains, Value = "lph" }, a.Id, b.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "s", Operation = GenericFilterOperation.StartsWith, Value = "alph" }, a.Id, b.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "s", Operation = GenericFilterOperation.EndsWith, Value = "bet" }, b.Id);

        // Numeric operations (int -> ValueInt64)
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.Eq, Value = 20 }, b.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.Neq, Value = 20 }, a.Id, cJob.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.In, Values = new object?[] { 10, 30 } }, a.Id, cJob.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.Gt, Value = 10 }, b.Id, cJob.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.Gte, Value = 20 }, b.Id, cJob.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.Lt, Value = 30 }, a.Id, b.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "n", Operation = GenericFilterOperation.Lte, Value = 20 }, a.Id, b.Id);

        // DateTime operations (DateTime -> ValueDateTime)
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.Eq, Value = t0.AddDays(1) }, b.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.Neq, Value = t0.AddDays(1) }, a.Id, cJob.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.In, Values = new object?[] { t0, t0.AddDays(2) } }, a.Id, cJob.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.Gt, Value = t0 }, b.Id, cJob.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.Gte, Value = t0.AddDays(2) }, cJob.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.Lt, Value = t0.AddDays(2) }, a.Id, b.Id);
        await AssertMetadataFilter(def, new GenericRecordValueFilter { Key = "dt", Operation = GenericFilterOperation.Lte, Value = t0.AddDays(1) }, a.Id, b.Id);
    }

    internal async Task AssertMetadataFilter(string jobDefinitionId, GenericRecordValueFilter filter, params Guid[] expectedIds)
    {
        var criteria = new JobQueryCriteria
        {
            JobDefinitionId = jobDefinitionId,
            CountLimit = 100,
            MetadataFilters = new List<GenericRecordValueFilter> { filter }
        };

        var queried = await Fixture.MasterJobs.QueryAsync(criteria);
        var ids = queried.Select(x => x.Id).ToHashSet();

        Assert.Equal(expectedIds.OrderBy(x => x).ToList(), ids.OrderBy(x => x).ToList());
    }

    internal virtual JobRawModel NewJob(
        string? jobDefinitionId = null,
        JobMasterJobStatus status = JobMasterJobStatus.HeldOnMaster,
        DateTime? scheduledAt = null,
        string? workerLane = null)
    {
        var now = DateTime.UtcNow;
        var sched = scheduledAt ?? now;

        return new JobRawModel(Fixture.ClusterId)
        {
            Id = Guid.NewGuid(),
            JobDefinitionId = jobDefinitionId ?? ("job-def-" + Guid.NewGuid()),
            TriggerSourceType = JobSchedulingTriggerSourceType.Once,
            Priority = JobMasterPriority.Medium,
            OriginalScheduledAt = sched,
            ScheduledAt = sched,
            Status = status,
            Timeout = TimeSpan.FromSeconds(10),
            MaxNumberOfRetries = 0,
            MsgData = "{}",
            CreatedAt = now,
            WorkerLane = workerLane
        };
    }

    private static void AssertJobEquivalent(JobRawModel expected, JobRawModel actual)
    {
        Assert.Equal(expected.ClusterId, actual.ClusterId);

        Assert.Equal(expected.Id, actual.Id);
        Assert.Equal(expected.JobDefinitionId, actual.JobDefinitionId);
        Assert.Equal(expected.TriggerSourceType, actual.TriggerSourceType);

        Assert.Equal(expected.BucketId, actual.BucketId);
        Assert.Equal(expected.AgentConnectionId?.IdValue, actual.AgentConnectionId?.IdValue);
        Assert.Equal(expected.AgentWorkerId, actual.AgentWorkerId);

        Assert.Equal(expected.Priority, actual.Priority);
        AssertDateTimeEquivalent(ToUtc(expected.OriginalScheduledAt), ToUtc(actual.OriginalScheduledAt));
        AssertDateTimeEquivalent(ToUtc(expected.ScheduledAt), ToUtc(actual.ScheduledAt));

        AssertJsonEquivalent(expected.MsgData, actual.MsgData);
        AssertJsonEquivalent(expected.Metadata, actual.Metadata);

        Assert.Equal(expected.Status, actual.Status);
        Assert.Equal(expected.NumberOfFailures, actual.NumberOfFailures);
        Assert.Equal(expected.Timeout, actual.Timeout);
        Assert.Equal(expected.MaxNumberOfRetries, actual.MaxNumberOfRetries);

        AssertDateTimeEquivalent(ToUtc(expected.CreatedAt), ToUtc(actual.CreatedAt));
        Assert.Equal(expected.RecurringScheduleId, actual.RecurringScheduleId);

        Assert.Equal(expected.PartitionLockId, actual.PartitionLockId);
        AssertDateTimeEquivalent(ToUtcN(expected.PartitionLockExpiresAt), ToUtcN(actual.PartitionLockExpiresAt));

        AssertDateTimeEquivalent(ToUtcN(expected.ProcessDeadline), ToUtcN(actual.ProcessDeadline));
        AssertDateTimeEquivalent(ToUtcN(expected.ProcessingStartedAt), ToUtcN(actual.ProcessingStartedAt));
        AssertDateTimeEquivalent(ToUtcN(expected.SucceedExecutedAt), ToUtcN(actual.SucceedExecutedAt));
        Assert.Equal(expected.WorkerLane, actual.WorkerLane);
    }

    private static DateTime ToUtc(DateTime dt) => DateTime.SpecifyKind(dt, DateTimeKind.Utc);
    private static DateTime? ToUtcN(DateTime? dt) => dt.HasValue ? DateTime.SpecifyKind(dt.Value, DateTimeKind.Utc) : null;

    private static void AssertDateTimeEquivalent(DateTime expectedUtc, DateTime actualUtc)
    {
        // Postgres timestamp precision is microseconds; .NET DateTime is 100ns ticks.
        // Allow <= 1 microsecond difference.
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

        using var expectedDoc = JsonDocument.Parse(expectedJson!);
        using var actualDoc = JsonDocument.Parse(actualJson!);

        Assert.True(JsonElement.DeepEquals(expectedDoc.RootElement, actualDoc.RootElement));
    }

    [Fact]
    public async Task PurgeFinalByScheduledAtAsync_ShouldDelete_OnlyFinalJobsOlderThanCutoff()
    {
        var def = "defPurgeFinal-" + Guid.NewGuid();
        var baseTime = DateTime.UtcNow.AddHours(-10);
        var cutoff = baseTime.AddMinutes(5);

        var oldSucceeded = NewJob(jobDefinitionId: def, status: JobMasterJobStatus.Succeeded, scheduledAt: baseTime.AddMinutes(1));
        oldSucceeded.SucceedExecutedAt = baseTime.AddMinutes(1);

        var oldFailed = NewJob(jobDefinitionId: def, status: JobMasterJobStatus.Failed, scheduledAt: baseTime.AddMinutes(3));
        oldFailed.SucceedExecutedAt = baseTime.AddMinutes(3);

        var recentSucceeded = NewJob(jobDefinitionId: def, status: JobMasterJobStatus.Succeeded, scheduledAt: baseTime.AddMinutes(10));
        recentSucceeded.SucceedExecutedAt = baseTime.AddMinutes(10);

        var heldOnMaster = NewJob(jobDefinitionId: def, status: JobMasterJobStatus.HeldOnMaster, scheduledAt: baseTime.AddMinutes(1));

        await Fixture.MasterJobs.AddAsync(oldSucceeded);
        await Fixture.MasterJobs.AddAsync(oldFailed);
        await Fixture.MasterJobs.AddAsync(recentSucceeded);
        await Fixture.MasterJobs.AddAsync(heldOnMaster);

        var deleted = await Fixture.MasterJobs.PurgeFinalByScheduledAtAsync(cutoff, limit: 100);
        Assert.True(deleted >= 2, $"Expected at least 2 deleted, got {deleted}");

        var remaining = await Fixture.MasterJobs.QueryAsync(new JobQueryCriteria
        {
            JobDefinitionId = def,
            CountLimit = 100
        });

        Assert.DoesNotContain(remaining, j => j.Id == oldSucceeded.Id);
        Assert.DoesNotContain(remaining, j => j.Id == oldFailed.Id);
        Assert.Contains(remaining, j => j.Id == recentSucceeded.Id);
        Assert.Contains(remaining, j => j.Id == heldOnMaster.Id);
    }

    [Fact]
    public async Task PurgeFinalByScheduledAtAsync_ShouldRespect_Limit()
    {
        var def = "defPurgeFinalLimit-" + Guid.NewGuid();
        var baseTime = DateTime.UtcNow.AddHours(-10);
        var cutoff = baseTime.AddMinutes(50);

        for (var i = 0; i < 10; i++)
        {
            var j = NewJob(jobDefinitionId: def, status: JobMasterJobStatus.Succeeded, scheduledAt: baseTime.AddMinutes(i));
            j.SucceedExecutedAt = baseTime.AddMinutes(i);
            await Fixture.MasterJobs.AddAsync(j);
        }

        var deleted = await Fixture.MasterJobs.PurgeFinalByScheduledAtAsync(cutoff, limit: 3);
        Assert.True(deleted <= 3, $"Expected at most 3 deleted, got {deleted}");
        Assert.True(deleted >= 1, $"Expected at least 1 deleted, got {deleted}");

        var remaining = await Fixture.MasterJobs.QueryAsync(new JobQueryCriteria
        {
            JobDefinitionId = def,
            CountLimit = 100
        });
        Assert.True(remaining.Count >= 7, $"Expected at least 7 remaining, got {remaining.Count}");
    }

    [Fact]
    public async Task PurgeFinalByScheduledAtAsync_ShouldNotDelete_NonFinalJobs()
    {
        var def = "defPurgeNonFinal-" + Guid.NewGuid();
        var baseTime = DateTime.UtcNow.AddHours(-10);
        var cutoff = baseTime.AddMinutes(50);

        var heldOnMaster = NewJob(jobDefinitionId: def, status: JobMasterJobStatus.HeldOnMaster, scheduledAt: baseTime.AddMinutes(1));
        var assignedToBucket = NewJob(jobDefinitionId: def, status: JobMasterJobStatus.AssignedToBucket, scheduledAt: baseTime.AddMinutes(2));
        var processing = NewJob(jobDefinitionId: def, status: JobMasterJobStatus.Processing, scheduledAt: baseTime.AddMinutes(3));
        var pendingRetry = NewJob(jobDefinitionId: def, status: JobMasterJobStatus.Queued, scheduledAt: baseTime.AddMinutes(4));

        await Fixture.MasterJobs.AddAsync(heldOnMaster);
        await Fixture.MasterJobs.AddAsync(assignedToBucket);
        await Fixture.MasterJobs.AddAsync(processing);
        await Fixture.MasterJobs.AddAsync(pendingRetry);

        var deleted = await Fixture.MasterJobs.PurgeFinalByScheduledAtAsync(cutoff, limit: 100);

        var remaining = await Fixture.MasterJobs.QueryAsync(new JobQueryCriteria
        {
            JobDefinitionId = def,
            CountLimit = 100
        });

        Assert.Contains(remaining, j => j.Id == heldOnMaster.Id);
        Assert.Contains(remaining, j => j.Id == assignedToBucket.Id);
        Assert.Contains(remaining, j => j.Id == processing.Id);
        Assert.Contains(remaining, j => j.Id == pendingRetry.Id);
    }

    [Fact]
    public async Task PurgeFinalByScheduledAtAsync_ShouldDelete_AllFinalStatuses()
    {
        var def = "defPurgeAllFinal-" + Guid.NewGuid();
        var baseTime = DateTime.UtcNow.AddHours(-10);
        var cutoff = baseTime.AddMinutes(50);

        var succeeded = NewJob(jobDefinitionId: def, status: JobMasterJobStatus.Succeeded, scheduledAt: baseTime.AddMinutes(1));
        var failed = NewJob(jobDefinitionId: def, status: JobMasterJobStatus.Failed, scheduledAt: baseTime.AddMinutes(2));
        var canceled = NewJob(jobDefinitionId: def, status: JobMasterJobStatus.Cancelled, scheduledAt: baseTime.AddMinutes(3));

        await Fixture.MasterJobs.AddAsync(succeeded);
        await Fixture.MasterJobs.AddAsync(failed);
        await Fixture.MasterJobs.AddAsync(canceled);

        var deleted = await Fixture.MasterJobs.PurgeFinalByScheduledAtAsync(cutoff, limit: 100);
        Assert.True(deleted >= 3, $"Expected at least 3 deleted, got {deleted}");

        var remaining = await Fixture.MasterJobs.QueryAsync(new JobQueryCriteria
        {
            JobDefinitionId = def,
            CountLimit = 100
        });

        Assert.DoesNotContain(remaining, j => j.Id == succeeded.Id);
        Assert.DoesNotContain(remaining, j => j.Id == failed.Id);
        Assert.DoesNotContain(remaining, j => j.Id == canceled.Id);
    }

    private static JobRawModel Clone(JobRawModel job)
    {
        return new JobRawModel(job.ClusterId)
        {
            Id = job.Id,
            JobDefinitionId = job.JobDefinitionId,
            TriggerSourceType = job.TriggerSourceType,
            BucketId = job.BucketId,
            AgentConnectionId = job.AgentConnectionId,
            AgentWorkerId = job.AgentWorkerId,
            Priority = job.Priority,
            OriginalScheduledAt = job.OriginalScheduledAt,
            ScheduledAt = job.ScheduledAt,
            MsgData = job.MsgData,
            Metadata = job.Metadata,
            Status = job.Status,
            NumberOfFailures = job.NumberOfFailures,
            Timeout = job.Timeout,
            MaxNumberOfRetries = job.MaxNumberOfRetries,
            CreatedAt = job.CreatedAt,
            RecurringScheduleId = job.RecurringScheduleId,
            PartitionLockId = job.PartitionLockId,
            PartitionLockExpiresAt = job.PartitionLockExpiresAt,
            ProcessDeadline = job.ProcessDeadline,
            ProcessingStartedAt = job.ProcessingStartedAt,
            SucceedExecutedAt = job.SucceedExecutedAt,
            WorkerLane = job.WorkerLane,
            Version = job.Version
        };
    }
}
