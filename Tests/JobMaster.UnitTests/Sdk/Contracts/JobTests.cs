using FluentAssertions;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Sdk.Abstractions.Jobs;

namespace JobMaster.UnitTests.Sdk.Contracts;

public class JobTests
{
    [Fact]
    public void New_WhenHandlerHasAttributes_UsesAttributesByDefault()
    {
        var clusterId = "cluster";
        var scheduledAt = new DateTime(2025, 01, 02, 03, 04, 05, DateTimeKind.Utc);

        var job = Job.New(clusterId, typeof(JobHandlerForTestAllAttributes), scheduledAt: scheduledAt);

        job.ClusterId.Should().Be(clusterId);
        job.JobDefinitionId.Should().Be("JobHandlerForTest");
        job.Status.Should().Be(JobMasterJobStatus.SavePending);
        job.ScheduleSourceType.Should().Be(JobSchedulingSourceType.Once);
        job.ScheduledAt.Should().Be(scheduledAt);
        job.OriginalScheduledAt.Should().Be(scheduledAt);
        job.Priority.Should().Be(JobMasterPriority.Low);
        job.Timeout.Should().Be(TimeSpan.FromSeconds(10));
        job.MaxNumberOfRetries.Should().Be(10);
        job.WorkerLane.Should().Be("Lane1");

        job.Metadata.Should().NotBeNull();
        job.Metadata!.ToReadable().GetLongValue("Test1").Should().Be(10L);
        job.Metadata!.ToReadable().GetIntValue("Test2").Should().Be(10);
        job.Metadata!.ToReadable().GetStringValue("TestStr").Should().Be("abc");
        job.Metadata!.ToReadable().GetShortValue("TestShort").Should().Be(12);
        job.Metadata!.ToReadable().GetByteValue("TestByte").Should().Be(9);
        job.Metadata!.ToReadable().GetDoubleValue("TestDouble").Should().BeApproximately(1.5, 0.0000001);
        job.Metadata!.ToReadable().GetDecimalValue("TestDecimal").Should().Be(12.34m);
        job.Metadata!.ToReadable().GetBoolValue("TestBool").Should().BeTrue();
        job.Metadata!.ToReadable().GetCharValue("TestChar").Should().Be('Z');

    }

    [Fact]
    public void New_WhenOverridesProvided_UsesOverrides()
    {
        var clusterId = "cluster";
        var scheduledAt = new DateTime(2025, 01, 02, 03, 04, 05, DateTimeKind.Utc);
        var metadata = WritableMetadata.New()
            .SetIntValue("Test2", 999)
            .SetStringValue("Custom", "abc");

        var job = Job.New(
            clusterId,
            typeof(JobHandlerForTestAllAttributes),
            scheduledAt: scheduledAt,
            priority: JobMasterPriority.High,
            timeout: TimeSpan.FromSeconds(42),
            maxNumberOfRetries: 2,
            writableMetadata: metadata,
            workerLane: "LaneX");

        job.Priority.Should().Be(JobMasterPriority.High);
        job.Timeout.Should().Be(TimeSpan.FromSeconds(42));
        job.MaxNumberOfRetries.Should().Be(2);
        job.WorkerLane.Should().Be("LaneX");

        job.Metadata.Should().NotBeNull();
        job.Metadata!.ToReadable().GetIntValue("Test2").Should().Be(999);
        job.Metadata!.ToReadable().GetStringValue("Custom").Should().Be("abc");
    }

    [Fact]
    public void New_WhenHandlerHasNoAttributes_UsesDefaults()
    {
        var clusterId = "cluster";
        var job = Job.New(clusterId, typeof(JobHandlerForTestNoAttributes));

        job.Priority.Should().Be(JobMasterPriority.Medium);
        job.Timeout.Should().Be(TimeSpan.FromMinutes(5));
        job.MaxNumberOfRetries.Should().Be(3);
        job.WorkerLane.Should().BeNull();
        job.JobDefinitionId.Should().Be(typeof(JobHandlerForTestNoAttributes).FullName);
    }

    [Fact]
    public void New_GenericOverload_MatchesNonGeneric()
    {
        var clusterId = "cluster";
        var scheduledAt = new DateTime(2025, 01, 02, 03, 04, 05, DateTimeKind.Utc);

        var nonGeneric = Job.New(clusterId, typeof(JobHandlerForTestAllAttributes), scheduledAt: scheduledAt);
        var generic = Job.New<JobHandlerForTestAllAttributes>(clusterId, scheduledAt: scheduledAt);

        generic.JobDefinitionId.Should().Be(nonGeneric.JobDefinitionId);
        generic.Priority.Should().Be(nonGeneric.Priority);
        generic.Timeout.Should().Be(nonGeneric.Timeout);
        generic.MaxNumberOfRetries.Should().Be(nonGeneric.MaxNumberOfRetries);
        generic.WorkerLane.Should().Be(nonGeneric.WorkerLane);
        generic.ScheduledAt.Should().Be(nonGeneric.ScheduledAt);
        generic.OriginalScheduledAt.Should().Be(nonGeneric.OriginalScheduledAt);
    }

    [Fact]
    public void FromRecurringSchedule_CopiesSchedulingAndMergesMetadataWithJobWinning()
    {
        var clusterId = "cluster";
        var scheduleAt = new DateTime(2025, 01, 02, 03, 04, 05, DateTimeKind.Utc);

        var recurringMetadata = WritableMetadata.New()
            .SetIntValue("Test2", 111)
            .SetStringValue("RecurringOnly", "ro");

        var recurring = RecurringSchedule.New(
            clusterId,
            jobDefinitionId: "ignored",
            values: WriteableMessageData.New().SetStringValue("k", "v"),
            expression: new NeverRecursCompiledExpr(),
            priority: JobMasterPriority.High,
            timeout: TimeSpan.FromSeconds(9),
            maxNumberOfRetries: 1,
            metadata: recurringMetadata,
            recurringScheduleType: RecurringScheduleType.Static,
            staticDefinitionId: "sd",
            startAfter: null,
            endBefore: null,
            workerLane: "LaneR");

        var job = Job.FromRecurringSchedule(clusterId, typeof(JobHandlerForTestAllAttributes), recurring, scheduleAt);

        job.ScheduleSourceType.Should().Be(JobSchedulingSourceType.StaticRecurring);
        job.RecurringScheduleId.Should().Be(recurring.Id);
        job.ScheduledAt.Should().Be(scheduleAt);
        job.OriginalScheduledAt.Should().Be(scheduleAt);
        job.Priority.Should().Be(JobMasterPriority.High);
        job.Timeout.Should().Be(TimeSpan.FromSeconds(9));
        job.MaxNumberOfRetries.Should().Be(1);
        job.WorkerLane.Should().Be("LaneR");

        job.Metadata.Should().NotBeNull();
        job.Metadata!.ToReadable().GetStringValue("RecurringOnly").Should().Be("ro");
        job.Metadata!.ToReadable().GetIntValue("Test2").Should().Be(10);
    }

    [Fact]
    public void ToModel_ThenFromModel_RoundTripsValues()
    {
        var clusterId = "cluster";
        var scheduledAt = new DateTime(2025, 01, 02, 03, 04, 05, DateTimeKind.Utc);
        var msg = WriteableMessageData.New().SetIntValue("i", 123);
        var metadata = WritableMetadata.New().SetStringValue("m", "x");

        var job = Job.New(
            clusterId,
            typeof(JobHandlerForTestAllAttributes),
            data: msg,
            scheduledAt: scheduledAt,
            priority: JobMasterPriority.High,
            timeout: TimeSpan.FromSeconds(7),
            maxNumberOfRetries: 2,
            writableMetadata: metadata,
            workerLane: "L");

        var raw = job.ToModel();
        var job2 = Job.FromModel(raw);

        job2.Id.Should().Be(job.Id);
        job2.ClusterId.Should().Be(job.ClusterId);
        job2.JobDefinitionId.Should().Be(job.JobDefinitionId);
        job2.ScheduleSourceType.Should().Be(job.ScheduleSourceType);
        job2.Status.Should().Be(job.Status);
        job2.OriginalScheduledAt.Should().Be(job.OriginalScheduledAt);
        job2.ScheduledAt.Should().Be(job.ScheduledAt);
        job2.Priority.Should().Be(job.Priority);
        job2.Timeout.Should().Be(job.Timeout);
        job2.MaxNumberOfRetries.Should().Be(job.MaxNumberOfRetries);
        job2.WorkerLane.Should().Be(job.WorkerLane);

        job2.MsgData.ToReadable().GetIntValue("i").Should().Be(123);
        job2.Metadata.Should().NotBeNull();
        job2.Metadata!.ToReadable().GetStringValue("m").Should().Be("x");
    }

    [Fact]
    public void New_WhenMaxNumberOfRetriesGreaterThan10_Throws()
    {
        var clusterId = "cluster";

        var act = () => Job.New(clusterId, typeof(JobHandlerForTestHighRetriesAttribute));
        act.Should().Throw<ArgumentException>().WithMessage("*MaxNumberOfRetries*");
    }

    [Fact]
    public void New_WhenMetadataContainsAllSupportedTypes_CanReadValues()
    {
        var clusterId = "cluster";
        var guid = Guid.Parse("8e8fd3b4-1c3b-4a2b-9d86-3c28b7c7f7b1");
        var dt = new DateTime(2025, 01, 02, 03, 04, 05, DateTimeKind.Utc);
        var bytes = new byte[] { 1, 2, 3, 4 };

        var metadata = WritableMetadata.New()
            .SetStringValue("str", "hello")
            .SetIntValue("int", 123)
            .SetLongValue("long", 1234567890123L)
            .SetShortValue("short", (short)12)
            .SetByteValue("byte", (byte)9)
            .SetCharValue("char", 'Z')
            .SetBoolValue("bool", true)
            .SetDoubleValue("double", 1.5)
            .SetDecimalValue("decimal", 12.34m)
            .SetUtcDateTimeValue("dt", dt)
            .SetGuidValue("guid", guid)
            .SetEnumValue("enum", JobMasterPriority.High)
            .SetByteArrayValue("bytes", bytes);

        var job = Job.New(
            clusterId,
            typeof(JobHandlerForTestNoAttributes),
            writableMetadata: metadata);

        var r = job.Metadata!.ToReadable();
        r.GetStringValue("str").Should().Be("hello");
        r.GetIntValue("int").Should().Be(123);
        r.GetLongValue("long").Should().Be(1234567890123L);
        r.GetShortValue("short").Should().Be(12);
        r.GetByteValue("byte").Should().Be(9);
        r.GetCharValue("char").Should().Be('Z');
        r.GetBoolValue("bool").Should().BeTrue();
        r.GetDoubleValue("double").Should().BeApproximately(1.5, 0.0000001);
        r.GetDecimalValue("decimal").Should().Be(12.34m);
        r.GetDateTimeValue("dt").Should().Be(dt);
        r.GetGuidValue("guid").Should().Be(guid);
        r.GetEnumValue<JobMasterPriority>("enum").Should().Be(JobMasterPriority.High);
        r.GetByteArrayValue("bytes").Should().Equal(bytes);
    }

    [Fact]
    public void ToModel_ThenFromModel_WhenMetadataContainsPrimitiveTypes_RoundTrips()
    {
        var clusterId = "cluster";
        var guid = Guid.Parse("8e8fd3b4-1c3b-4a2b-9d86-3c28b7c7f7b1");
        var dt = new DateTime(2025, 01, 02, 03, 04, 05, DateTimeKind.Utc);

        var metadata = WritableMetadata.New()
            .SetStringValue("str", "hello")
            .SetIntValue("int", 123)
            .SetLongValue("long", 1234567890123L)
            .SetShortValue("short", (short)12)
            .SetByteValue("byte", (byte)9)
            .SetCharValue("char", 'Z')
            .SetBoolValue("bool", true)
            .SetDoubleValue("double", 1.5)
            .SetDecimalValue("decimal", 12.34m)
            .SetUtcDateTimeValue("dt", dt)
            .SetGuidValue("guid", guid)
            .SetEnumValue("enum", JobMasterPriority.High);

        var job = Job.New(
            clusterId,
            typeof(JobHandlerForTestNoAttributes),
            writableMetadata: metadata);

        var job2 = Job.FromModel(job.ToModel());
        var r = job2.Metadata!.ToReadable();

        r.GetStringValue("str").Should().Be("hello");
        r.GetIntValue("int").Should().Be(123);
        r.GetLongValue("long").Should().Be(1234567890123L);
        r.GetShortValue("short").Should().Be(12);
        r.GetByteValue("byte").Should().Be(9);
        r.GetCharValue("char").Should().Be('Z');
        r.GetBoolValue("bool").Should().BeTrue();
        r.GetDoubleValue("double").Should().BeApproximately(1.5, 0.0000001);
        r.GetDecimalValue("decimal").Should().Be(12.34m);
        r.GetDateTimeValue("dt").ToUniversalTime().Should().Be(dt);
        r.GetGuidValue("guid").Should().Be(guid);
        r.GetEnumValue<JobMasterPriority>("enum").Should().Be(JobMasterPriority.High);
    }
    
    [JobMasterDefinitionId("JobHandlerForTest")]
    [JobMasterPriority(JobMasterPriority.Low)]
    [JobMasterWorkerLane("Lane1")]
    [JobMasterMaxNumberOfRetries(10)]
    [JobMasterTimeout(10)]
    [JobMasterMetadata("Test1", 10L)]
    [JobMasterMetadata("Test2", 10)]
    [JobMasterMetadata("TestEnum", (int)MyEnum.Opt2)]
    [JobMasterMetadata("TestStr", "abc")]
    [JobMasterMetadata("TestShort", (short)12)]
    [JobMasterMetadata("TestByte", (byte)9)]
    [JobMasterMetadata("TestDouble", 1.5)]
    [JobMasterMetadata("TestDecimal", 12.34)]
    [JobMasterMetadata("TestBool", true)]
    [JobMasterMetadata("TestChar", 'Z')]
    private class JobHandlerForTestAllAttributes : IJobHandler
    {
        public Task HandleAsync(JobContext job)
        {
            return Task.CompletedTask;
        }
    }

    private class JobHandlerForTestNoAttributes : IJobHandler
    {
        public Task HandleAsync(JobContext job)
        {
            return Task.CompletedTask;
        }
    }

    [JobMasterMaxNumberOfRetries(11)]
    private class JobHandlerForTestHighRetriesAttribute : IJobHandler
    {
        public Task HandleAsync(JobContext job)
        {
            return Task.CompletedTask;
        }
    }
    
    private enum MyEnum
    {
        Opt1,
        Opt2,
        Opt3
    }
}

