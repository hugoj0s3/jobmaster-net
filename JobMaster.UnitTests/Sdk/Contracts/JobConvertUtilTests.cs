using System.Reflection;
using FluentAssertions;
using JobMaster.Contracts;
using JobMaster.Contracts.Models;
using JobMaster.Sdk.Contracts.Jobs;
using JobMaster.Sdk.Contracts.Models.Agents;
using JobMaster.Sdk.Contracts.Models.GenericRecords;
using JobMaster.Sdk.Contracts.Models.Jobs;
using JobMaster.Sdk.Contracts.Serialization;

namespace JobMaster.UnitTests.Sdk.Contracts;

public class JobConvertUtilTests
{
    [Fact]
    public void JobAndJobRawModel_ShouldHaveMatchingPublicPropertyNames_ForConversion()
    {
        var jobProps = GetRelevantPublicPropertyNames(typeof(Job), "IsValid");
        var rawProps = GetRelevantPublicPropertyNames(typeof(JobRawModel), "IsValid");

        var missingOnJob = rawProps.Except(jobProps).OrderBy(x => x).ToArray();
        var missingOnRaw = jobProps.Except(rawProps).OrderBy(x => x).ToArray();

        missingOnJob.Should().BeEmpty("every public property on JobRawModel should exist on Job to keep conversions aligned");
        missingOnRaw.Should().BeEmpty("every public property on Job should exist on JobRawModel to keep conversions aligned");
    }

    [Fact]
    public void ToJob_ThenToJobRawModel_ShouldRoundTripAllJobRawModelProperties()
    {
        var raw = new JobRawModel("c")
        {
            Id = Guid.Parse("b10c8e9a-0b2f-4c9f-88ea-3d7f7ac6f4d0"),
            JobDefinitionId = "def",
            ScheduleSourceType = JobSchedulingSourceType.Once,
            BucketId = "bucket",
            AgentConnectionId = new AgentConnectionId("c", "a"),
            AgentWorkerId = "w",
            Priority = JobMasterPriority.High,
            OriginalScheduledAt = new DateTime(2025, 01, 02, 03, 04, 05, DateTimeKind.Utc),
            ScheduledAt = new DateTime(2025, 01, 02, 03, 04, 06, DateTimeKind.Utc),
            MsgData = "{\"i\":123}",
            Metadata = "{\"m\":\"x\"}",
            Status = JobMasterJobStatus.HeldOnMaster,
            NumberOfFailures = 2,
            Timeout = TimeSpan.FromSeconds(7),
            MaxNumberOfRetries = 3,
            CreatedAt = new DateTime(2025, 01, 02, 03, 04, 07, DateTimeKind.Utc),
            RecurringScheduleId = Guid.Parse("9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d"),
            PartitionLockId = 12,
            PartitionLockExpiresAt = new DateTime(2025, 01, 02, 03, 05, 00, DateTimeKind.Utc),
            ProcessDeadline = new DateTime(2025, 01, 02, 03, 06, 00, DateTimeKind.Utc),
            WorkerLane = "lane"
        };

        var job = JobConvertUtil.ToJob(raw);
        var raw2 = JobConvertUtil.ToJobRawModel(job);

        foreach (var prop in GetRelevantPublicProperties(typeof(JobRawModel), "IsValid"))
        {
            if (IgnoredRoundTripPropertyNames.Contains(prop.Name))
                continue;

            var expected = prop.GetValue(raw);
            var actual = prop.GetValue(raw2);

            if (prop.Name is "MsgData" or "Metadata")
            {
                var expectedDict = InternalJobMasterSerializer.Deserialize<Dictionary<string, object?>>(expected as string ?? "{}");
                var actualDict = InternalJobMasterSerializer.Deserialize<Dictionary<string, object?>>(actual as string ?? "{}");
                actualDict.Should().BeEquivalentTo(expectedDict, $"{prop.Name} should round-trip as a dictionary");
                continue;
            }

            if (prop.PropertyType == typeof(AgentConnectionId))
            {
                ((AgentConnectionId?)actual)?.IdValue.Should().Be(((AgentConnectionId?)expected)?.IdValue, $"{prop.Name} should round-trip");
                continue;
            }

            if (prop.PropertyType == typeof(DateTime))
            {
                ((DateTime)actual!).ToUniversalTime().Should().Be(((DateTime)expected!).ToUniversalTime(), $"{prop.Name} should round-trip (UTC)");
                continue;
            }

            if (prop.PropertyType == typeof(DateTime?))
            {
                ((DateTime?)actual)?.ToUniversalTime().Should().Be(((DateTime?)expected)?.ToUniversalTime(), $"{prop.Name} should round-trip (UTC)");
                continue;
            }

            actual.Should().Be(expected, $"{prop.Name} should round-trip");
        }
    }

    [Fact]
    public void JobRawModel_And_JobPersistenceRecord_ShouldRoundTripAllPersistenceProperties()
    {
        var record = new JobPersistenceRecord
        {
            ClusterId = "c",
            Id = Guid.Parse("b10c8e9a-0b2f-4c9f-88ea-3d7f7ac6f4d0"),
            JobDefinitionId = "def",
            ScheduledType = (int)JobSchedulingSourceType.Once,
            BucketId = "bucket",
            AgentConnectionId = "c:a",
            AgentWorkerId = "w",
            Priority = (int)JobMasterPriority.High,
            OriginalScheduledAt = new DateTime(2025, 01, 02, 03, 04, 05, DateTimeKind.Utc),
            ScheduledAt = new DateTime(2025, 01, 02, 03, 04, 06, DateTimeKind.Utc),
            MsgData = "{\"i\":123}",
            Metadata = GenericRecordEntry.FromWritableMetadata(
                "c",
                "g",
                "e",
                WritableMetadata.New().SetStringValue("m", "x")),
            Status = (int)JobMasterJobStatus.HeldOnMaster,
            NumberOfFailures = 2,
            TimeoutTicks = TimeSpan.FromSeconds(7).Ticks,
            MaxNumberOfRetries = 3,
            CreatedAt = new DateTime(2025, 01, 02, 03, 04, 07, DateTimeKind.Utc),
            RecurringScheduleId = Guid.Parse("9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d"),
            PartitionLockId = 12,
            PartitionLockExpiresAt = new DateTime(2025, 01, 02, 03, 05, 00, DateTimeKind.Utc),
            ProcessDeadline = new DateTime(2025, 01, 02, 03, 06, 00, DateTimeKind.Utc),
            WorkerLane = "lane"
        };

        var raw = JobConvertUtil.FromPersistence(record);
        var record2 = JobConvertUtil.ToPersistence(raw);

        foreach (var prop in typeof(JobPersistenceRecord).GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            if (prop.GetIndexParameters().Length != 0)
                continue;

            var expected = prop.GetValue(record);
            var actual = prop.GetValue(record2);

            if (prop.Name == nameof(JobPersistenceRecord.Metadata))
            {
                var expectedDict = ((GenericRecordEntry?)expected)?.ToReadable().ToDictionary() ?? new Dictionary<string, object?>();
                var actualDict = ((GenericRecordEntry?)actual)?.ToReadable().ToDictionary() ?? new Dictionary<string, object?>();
                actualDict.Should().BeEquivalentTo(expectedDict, "Metadata should round-trip as a dictionary");
                continue;
            }

            if (prop.PropertyType == typeof(DateTime))
            {
                ((DateTime)actual!).ToUniversalTime().Should().Be(((DateTime)expected!).ToUniversalTime(), $"{prop.Name} should round-trip (UTC)");
                continue;
            }

            if (prop.PropertyType == typeof(DateTime?))
            {
                ((DateTime?)actual)?.ToUniversalTime().Should().Be(((DateTime?)expected)?.ToUniversalTime(), $"{prop.Name} should round-trip (UTC)");
                continue;
            }

            actual.Should().Be(expected, $"{prop.Name} should round-trip");
        }
    }

    [Fact]
    public void Job_ShouldExposeAllJobContextPropertyNames()
    {
        var jobPropNames = GetRelevantPublicPropertyNames(typeof(Job), "IsValid");
        var ctxPropNames = GetRelevantPublicPropertyNames(typeof(JobContext), "RecurringSchedule");

        var missingOnJob = ctxPropNames.Except(jobPropNames).OrderBy(x => x).ToArray();
        missingOnJob.Should().BeEmpty("Job should contain all JobContext properties (by name)");
    }

    private static readonly HashSet<string> IgnoredRoundTripPropertyNames = new(StringComparer.Ordinal)
    {
        "ClusterId"
    };

    private static IEnumerable<PropertyInfo> GetRelevantPublicProperties(Type t, params string[] ignoreNames)
    {
        return t.GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(p => p.GetIndexParameters().Length == 0)
            .Where(p => ignoreNames.Contains(p.Name) == false);
    }
    
    private static HashSet<string> GetRelevantPublicPropertyNames(Type t, params string[] ignoreNames)
    {
        return GetRelevantPublicProperties(t, ignoreNames)
            .Select(p => p.Name)
            .ToHashSet(StringComparer.Ordinal);
    }
}
