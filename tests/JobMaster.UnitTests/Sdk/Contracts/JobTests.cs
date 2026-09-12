using FluentAssertions;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models;

namespace JobMaster.UnitTests.Sdk.Contracts;

public class JobTests
{
    [Fact]
    public void New_WhenHandlerHasAttributes_UsesAttributesByDefault()
    {
        var clusterId = "cluster";
        var scheduledAt = new DateTime(2025, 01, 02, 03, 04, 05, DateTimeKind.Utc);

        var job = Job.New(clusterId, typeof(JobMasterHandlerForTestAllAttributes), scheduledAt: scheduledAt);

        job.ClusterId.Should().Be(clusterId);
        job.JobDefinitionId.Should().Be("JobHandlerForTest");
        job.Status.Should().Be(JobMasterJobStatus.PendingSave);
        job.TriggerSourceType.Should().Be(JobMasterTriggerSourceType.Once);
        job.NextPlanExecutionAt.Should().Be(scheduledAt);
        job.ScheduledAt.Should().Be(scheduledAt);
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
            typeof(JobMasterHandlerForTestAllAttributes),
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
        var job = Job.New(clusterId, typeof(JobMasterHandlerForTestNoAttributes));

        job.Priority.Should().Be(JobMasterPriority.Medium);
        job.Timeout.Should().Be(TimeSpan.FromMinutes(5));
        job.MaxNumberOfRetries.Should().Be(3);
        job.WorkerLane.Should().BeNull();
        job.JobDefinitionId.Should().Be(typeof(JobMasterHandlerForTestNoAttributes).FullName);
    }

    [Fact]
    public void New_GenericOverload_MatchesNonGeneric()
    {
        var clusterId = "cluster";
        var scheduledAt = new DateTime(2025, 01, 02, 03, 04, 05, DateTimeKind.Utc);

        var nonGeneric = Job.New(clusterId, typeof(JobMasterHandlerForTestAllAttributes), scheduledAt: scheduledAt);
        var generic = Job.New<JobMasterHandlerForTestAllAttributes>(clusterId, scheduledAt: scheduledAt);

        generic.JobDefinitionId.Should().Be(nonGeneric.JobDefinitionId);
        generic.Priority.Should().Be(nonGeneric.Priority);
        generic.Timeout.Should().Be(nonGeneric.Timeout);
        generic.MaxNumberOfRetries.Should().Be(nonGeneric.MaxNumberOfRetries);
        generic.WorkerLane.Should().Be(nonGeneric.WorkerLane);
        generic.NextPlanExecutionAt.Should().Be(nonGeneric.NextPlanExecutionAt);
        generic.ScheduledAt.Should().Be(nonGeneric.ScheduledAt);
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

        var job = Job.FromRecurringSchedule(clusterId, typeof(JobMasterHandlerForTestAllAttributes), recurring, scheduleAt);

        job.TriggerSourceType.Should().Be(JobMasterTriggerSourceType.StaticRecurring);
        job.SourceId.Should().Be(recurring.Id);
        job.NextPlanExecutionAt.Should().Be(scheduleAt);
        job.ScheduledAt.Should().Be(scheduleAt);
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
            typeof(JobMasterHandlerForTestAllAttributes),
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
        job2.TriggerSourceType.Should().Be(job.TriggerSourceType);
        job2.Status.Should().Be(job.Status);
        job2.ScheduledAt.Should().Be(job.ScheduledAt);
        job2.NextPlanExecutionAt.Should().Be(job.NextPlanExecutionAt);
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

        var act = () => Job.New(clusterId, typeof(JobMasterHandlerForTestHighRetriesAttribute));
        act.Should().Throw<ArgumentException>().WithMessage("*MaxNumberOfRetries*");
    }

    [Fact]
    public void New_WhenMetadataContainsAllSupportedTypes_CanReadValues()
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
            .SetDateTimeValue("dt", dt)
            .SetGuidValue("guid", guid);

        var job = Job.New(
            clusterId,
            typeof(JobMasterHandlerForTestNoAttributes),
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
    }

    [Fact]
    public void New_WhenScheduledAtOmitted_ScheduledAtAndNextPlanExecutionAt_DefaultToNow()
    {
        // Exercises NewBase's own defaulting (shared by both New(Type) and New(JobDefinitionConfig)) --
        // every other test either passes an explicit scheduledAt or doesn't assert these fields.
        var clusterId = "cluster";
        var before = DateTime.UtcNow;

        var job = Job.New(clusterId, typeof(JobMasterHandlerForTestNoAttributes));

        var after = DateTime.UtcNow;
        // ScheduledAt/NextPlanExecutionAt are two separate DateTime.UtcNow calls inside NewBase, not one
        // shared value -- close-to-now, not exactly equal to each other, is the real contract here.
        job.ScheduledAt.Should().BeOnOrAfter(before).And.BeOnOrBefore(after);
        job.NextPlanExecutionAt.Should().NotBeNull();
        job.NextPlanExecutionAt!.Value.Should().BeOnOrAfter(before).And.BeOnOrBefore(after);
    }

    [Fact]
    public void New_WhenDataOmitted_MsgDataDefaultsToEmpty()
    {
        var clusterId = "cluster";

        var job = Job.New(clusterId, typeof(JobMasterHandlerForTestNoAttributes));

        job.MsgData.Should().NotBeNull();
        job.MsgData.ToDictionary().Should().BeEmpty();
    }

    [Fact]
    public void New_ConfigOverload_WhenNoOverrides_UsesFrameworkDefaults()
    {
        var clusterId = "cluster";
        var config = new JobDefinitionConfig("orders.process.bare");

        var job = Job.New(clusterId, config);

        job.ClusterId.Should().Be(clusterId);
        job.JobDefinitionId.Should().Be("orders.process.bare");
        job.Priority.Should().Be(JobMasterPriority.Medium);
        job.Timeout.Should().Be(TimeSpan.FromMinutes(5));
        job.MaxNumberOfRetries.Should().Be(3);
        job.WorkerLane.Should().BeNull();
        job.Metadata.Should().NotBeNull();
        job.Metadata!.ToDictionary().Should().BeEmpty();
    }

    [Fact]
    public void New_ConfigOverload_WhenNoOverrides_FallsBackToMasterConfigDefaults()
    {
        var clusterId = "cluster";
        var config = new JobDefinitionConfig("orders.process.masterdefaults");
        var masterConfig = new ClusterConfigurationModel(clusterId)
        {
            DefaultJobTimeout = TimeSpan.FromSeconds(42),
            DefaultMaxOfRetryCount = 7,
        };

        var job = Job.New(clusterId, config, masterConfig: masterConfig);

        job.Timeout.Should().Be(TimeSpan.FromSeconds(42));
        job.MaxNumberOfRetries.Should().Be(7);
    }

    [Fact]
    public void New_ConfigOverload_WhenValuesProvided_UsesConfigValues()
    {
        var clusterId = "cluster";
        var metadata = WritableMetadata.New().SetStringValue("k", "v");
        var config = new JobDefinitionConfig(
            "orders.process.overrides",
            priority: JobMasterPriority.High,
            timeout: TimeSpan.FromSeconds(9),
            maxNumberOfRetries: 2,
            workerLane: "orders",
            metadata: metadata);

        var job = Job.New(clusterId, config);

        job.JobDefinitionId.Should().Be("orders.process.overrides");
        job.Priority.Should().Be(JobMasterPriority.High);
        job.Timeout.Should().Be(TimeSpan.FromSeconds(9));
        job.MaxNumberOfRetries.Should().Be(2);
        job.WorkerLane.Should().Be("orders");
        job.Metadata!.ToReadable().GetStringValue("k").Should().Be("v");
    }

    [Fact]
    public void New_ConfigOverload_WhenMaxNumberOfRetriesGreaterThan10_Throws()
    {
        var clusterId = "cluster";
        var config = new JobDefinitionConfig("orders.process.badretries", maxNumberOfRetries: 11);

        var act = () => Job.New(clusterId, config);
        act.Should().Throw<ArgumentException>().WithMessage("*MaxNumberOfRetries*");
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
            .SetDateTimeValue("dt", dt)
            .SetGuidValue("guid", guid);

        var job = Job.New(
            clusterId,
            typeof(JobMasterHandlerForTestNoAttributes),
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
        r.GetDateTimeValue("dt").Should().Be(dt);
        r.GetGuidValue("guid").Should().Be(guid);
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
    private class JobMasterHandlerForTestAllAttributes : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job)
        {
            return Task.CompletedTask;
        }
    }

    private class JobMasterHandlerForTestNoAttributes : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job)
        {
            return Task.CompletedTask;
        }
    }

    [JobMasterMaxNumberOfRetries(11)]
    private class JobMasterHandlerForTestHighRetriesAttribute : IJobMasterHandler
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

