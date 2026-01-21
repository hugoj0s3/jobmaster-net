using System.Reflection;
using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Serialization;

namespace JobMaster.UnitTests.Sdk.Contracts;

public class RecurringScheduleConvertUtilTests
{
    [Fact]
    public void RecurringScheduleAndRecurringScheduleRawModel_ShouldHaveMatchingPublicPropertyNames_ForConversion()
    {
        var scheduleProps = GetRelevantPublicPropertyNames(typeof(RecurringSchedule), "IsValid", "RecurExpression");
        var rawProps = GetRelevantPublicPropertyNames(typeof(RecurringScheduleRawModel), "IsValid", "Expression", "ExpressionTypeId");

        var missingOnSchedule = rawProps.Except(scheduleProps).OrderBy(x => x).ToArray();
        var missingOnRaw = scheduleProps.Except(rawProps).OrderBy(x => x).ToArray();

        missingOnSchedule.Should().BeEmpty("every public property on RecurringScheduleRawModel should exist on RecurringSchedule to keep conversions aligned");
        missingOnRaw.Should().BeEmpty("every public property on RecurringSchedule should exist on RecurringScheduleRawModel to keep conversions aligned");
    }

    [Fact]
    public void RecurringScheduleRawModel_And_RecurringSchedulePersistenceRecord_ShouldHaveMatchingPublicPropertyNames_ForConversion()
    {
        var rawProps = GetRelevantPublicPropertyNames(typeof(RecurringScheduleRawModel), "IsValid");
        var recordProps = GetRelevantPublicPropertyNames(typeof(RecurringSchedulePersistenceRecord));

        static string Normalize(string name)
        {
            return name switch
            {
                nameof(RecurringScheduleRawModel.Timeout) => nameof(RecurringSchedulePersistenceRecord.TimeoutTicks),
                _ => name
            };
        }

        var normalizedRaw = rawProps.Select(Normalize).ToHashSet(StringComparer.Ordinal);
        var normalizedRecord = recordProps.Select(Normalize).ToHashSet(StringComparer.Ordinal);

        var missingOnRecord = normalizedRaw.Except(normalizedRecord).OrderBy(x => x).ToArray();
        var missingOnRaw = normalizedRecord.Except(normalizedRaw).OrderBy(x => x).ToArray();

        missingOnRecord.Should().BeEmpty("every public property on RecurringScheduleRawModel should exist on RecurringSchedulePersistenceRecord (allowing intentional naming differences)");
        missingOnRaw.Should().BeEmpty("every public property on RecurringSchedulePersistenceRecord should exist on RecurringScheduleRawModel (allowing intentional naming differences)");
    }

    [Fact]
    public void ToRecurringSchedule_ThenToRawModel_ShouldRoundTripAllRecurringScheduleRawModelProperties()
    {
        var raw = new RecurringScheduleRawModel("c")
        {
            Id = Guid.Parse("b10c8e9a-0b2f-4c9f-88ea-3d7f7ac6f4d0"),
            Expression = "",
            ExpressionTypeId = NeverRecursExprCompiler.TypeId,
            JobDefinitionId = "def",
            StaticDefinitionId = "staticDef",
            ProfileId = "profile",
            Status = RecurringScheduleStatus.Active,
            RecurringScheduleType = RecurringScheduleType.Static,
            TerminatedAt = new DateTime(2025, 01, 02, 03, 04, 05, DateTimeKind.Utc),
            MsgData = "{\"i\":123}",
            Metadata = "{\"m\":\"x\"}",
            Priority = JobMasterPriority.High,
            MaxNumberOfRetries = 3,
            Timeout = TimeSpan.FromSeconds(7),
            BucketId = "bucket",
            AgentConnectionId = new AgentConnectionId("c", "a"),
            AgentWorkerId = "w",
            PartitionLockId = 12,
            PartitionLockExpiresAt = new DateTime(2025, 01, 02, 03, 05, 00, DateTimeKind.Utc),
            CreatedAt = new DateTime(2025, 01, 02, 03, 04, 07, DateTimeKind.Utc),
            StartAfter = new DateTime(2025, 01, 02, 03, 06, 00, DateTimeKind.Utc),
            EndBefore = new DateTime(2025, 01, 03, 00, 00, 00, DateTimeKind.Utc),
            LastPlanCoverageUntil = new DateTime(2025, 01, 05, 00, 00, 00, DateTimeKind.Utc),
            LastExecutedPlan = new DateTime(2025, 01, 04, 00, 00, 00, DateTimeKind.Utc),
            HasFailedOnLastPlanExecution = true,
            IsJobCancellationPending = true,
            StaticDefinitionLastEnsured = new DateTime(2025, 01, 06, 00, 00, 00, DateTimeKind.Utc),
            WorkerLane = "lane"
        };

        var entity = RecurringScheduleConvertUtil.ToRecurringSchedule(raw);
        var raw2 = RecurringScheduleConvertUtil.ToRawModel(entity);

        foreach (var prop in GetRelevantPublicProperties(typeof(RecurringScheduleRawModel), "IsValid"))
        {
            if (IgnoredRawRoundTripPropertyNames.Contains(prop.Name))
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
    public void RecurringScheduleRawModel_And_RecurringSchedulePersistenceRecord_ShouldRoundTripAllPersistenceProperties()
    {
        var record = new RecurringSchedulePersistenceRecord
        {
            ClusterId = "c",
            Id = Guid.Parse("b10c8e9a-0b2f-4c9f-88ea-3d7f7ac6f4d0"),
            Expression = "",
            ExpressionTypeId = NeverRecursExprCompiler.TypeId,
            JobDefinitionId = "def",
            StaticDefinitionId = "staticDef",
            ProfileId = "profile",
            Status = (int)RecurringScheduleStatus.Active,
            RecurringScheduleType = (int)RecurringScheduleType.Static,
            StaticDefinitionLastEnsured = new DateTime(2025, 01, 06, 00, 00, 00, DateTimeKind.Utc),
            TerminatedAt = new DateTime(2025, 01, 02, 03, 04, 05, DateTimeKind.Utc),
            MsgData = "{\"i\":123}",
            Metadata = GenericRecordEntry.FromWritableMetadata(
                "c",
                MasterGenericRecordGroupIds.RecurringScheduleMetadata,
                "e",
                WritableMetadata.New().SetStringValue("m", "x")),
            Priority = (int)JobMasterPriority.High,
            MaxNumberOfRetries = 3,
            TimeoutTicks = TimeSpan.FromSeconds(7).Ticks,
            BucketId = "bucket",
            AgentConnectionId = "c:a",
            AgentWorkerId = "w",
            PartitionLockId = 12,
            PartitionLockExpiresAt = new DateTime(2025, 01, 02, 03, 05, 00, DateTimeKind.Utc),
            CreatedAt = new DateTime(2025, 01, 02, 03, 04, 07, DateTimeKind.Utc),
            StartAfter = new DateTime(2025, 01, 02, 03, 06, 00, DateTimeKind.Utc),
            EndBefore = new DateTime(2025, 01, 03, 00, 00, 00, DateTimeKind.Utc),
            LastPlanCoverageUntil = new DateTime(2025, 01, 05, 00, 00, 00, DateTimeKind.Utc),
            LastExecutedPlan = new DateTime(2025, 01, 04, 00, 00, 00, DateTimeKind.Utc),
            HasFailedOnLastPlanExecution = true,
            IsJobCancellationPending = true,
            WorkerLane = "lane"
        };

        var raw = RecurringScheduleConvertUtil.FromPersistence(record);
        var record2 = RecurringScheduleConvertUtil.ToPersistence(raw);

        foreach (var prop in typeof(RecurringSchedulePersistenceRecord).GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            if (prop.GetIndexParameters().Length != 0)
                continue;

            var expected = prop.GetValue(record);
            var actual = prop.GetValue(record2);

            if (prop.Name == nameof(RecurringSchedulePersistenceRecord.Metadata))
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
    public void ToContext_ShouldMapExpectedFields()
    {
        var raw = new RecurringScheduleRawModel("c")
        {
            Id = Guid.Parse("b10c8e9a-0b2f-4c9f-88ea-3d7f7ac6f4d0"),
            Expression = "",
            ExpressionTypeId = NeverRecursExprCompiler.TypeId,
            JobDefinitionId = "def",
            ProfileId = "profile",
            CreatedAt = new DateTime(2025, 01, 02, 03, 04, 07, DateTimeKind.Utc),
            RecurringScheduleType = RecurringScheduleType.Dynamic,
            StaticDefinitionId = null,
            StartAfter = new DateTime(2025, 01, 02, 03, 06, 00, DateTimeKind.Utc),
            EndBefore = new DateTime(2025, 01, 03, 00, 00, 00, DateTimeKind.Utc),
            Metadata = "{\"m\":\"x\"}",
            WorkerLane = "lane"
        };

        var ctx = RecurringScheduleConvertUtil.ToContext(raw);

        ctx.Id.Should().Be(raw.Id);
        ctx.ClusterId.Should().Be(raw.ClusterId);
        ctx.ProfileId.Should().Be(raw.ProfileId);
        ctx.CreatedAt.ToUniversalTime().Should().Be(raw.CreatedAt.ToUniversalTime());
        ctx.RecurringScheduleType.Should().Be(raw.RecurringScheduleType);
        ctx.StaticDefinitionId.Should().Be(raw.StaticDefinitionId);
        ctx.JobDefinitionId.Should().Be(raw.JobDefinitionId);
        ctx.StartAfter.Should().Be(raw.StartAfter);
        ctx.EndBefore.Should().Be(raw.EndBefore);
        ctx.WorkerLane.Should().Be(raw.WorkerLane);
        ctx.Metadata.ToDictionary().Should().BeEquivalentTo(new Dictionary<string, object?> { ["m"] = "x" });
    }

    private static readonly HashSet<string> IgnoredRawRoundTripPropertyNames = new(StringComparer.Ordinal)
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
