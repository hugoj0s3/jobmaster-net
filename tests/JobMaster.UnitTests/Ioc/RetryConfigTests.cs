using FluentAssertions;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Ioc.Setup;
using JobMaster.Sdk.Utils;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.UnitTests.Ioc;

public class RetryConfigTests
{
    private static ClusterConfigBuilder Builder() => new(null, new ServiceCollection());

    [Fact]
    public void RetryDelays_UnderMinimum_Throws()
    {
        var act = () => Builder().RetryDelays(TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(10));

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void RetryDelays_Empty_Throws()
    {
        var act = () => Builder().RetryDelays();

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void RetryDelays_WithAnInterceptor_AreKept()
    {
        // Allowed together: the interceptor picks these with JobRetryDecision.UseRetryDelays().
        var b = Builder();
        b.RetryDelays(TimeSpan.FromMinutes(1)).InterceptRetry(_ => JobRetryDecision.UseRetryDelays());

        b.clusterDefinition.RetryDelays.Should().Equal(TimeSpan.FromMinutes(1));
        b.clusterDefinition.RetryInterceptor.Should().NotBeNull();
    }

    [Fact]
    public void InterceptRetry_SetTwice_Throws()
    {
        var b = Builder();
        b.InterceptRetry(_ => JobRetryDecision.UseRetryDelays());

        var act = () => b.InterceptRetry<NoOpInterceptor>();

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void Standalone_InterceptRetry_SetTwice_Throws()
    {
        var standalone = Builder().UseStandaloneCluster();
        standalone.InterceptRetry<NoOpInterceptor>();

        var act = () => standalone.InterceptRetry(_ => JobRetryDecision.UseRetryDelays());

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ConfigFromJson_RetryDelays_AreParsed()
    {
        const string json = """
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "RetryDelays": ["00:00:30", "00:05:00", "01:00:00"]
        }
        """;

        var b = Builder();
        b.ConfigFromJson(json);

        b.clusterDefinition.RetryDelays.Should().Equal(
            TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(5), TimeSpan.FromHours(1));
    }

    [Fact]
    public void ScheduleRetry_WaitsExactlyTheGivenDelay()
    {
        // No fixed extra wait on top of the delay any more (it used to add 10 minutes).
        var job = new JobRawModel("c1")
        {
            Id = JobMasterRandomUtil.NewGuid7(),
            JobDefinitionId = "job-def",
            ScheduledAt = DateTime.UtcNow.AddMinutes(-5),
        };

        job.ScheduleRetry(TimeSpan.FromSeconds(30));

        job.NextPlanExecutionAt.Should().BeCloseTo(DateTime.UtcNow.AddSeconds(30), TimeSpan.FromSeconds(5));
        job.NumberOfFailures.Should().Be(1);
    }

    [Fact]
    public void InterceptorDescription_DescribesWhatIsConfigured()
    {
        // Stored on master and shown by the API; a delegate can't be named, a type can.
        JobMasterRetrySettings.BuiltIn.InterceptorDescription.Should().Be("None");
        new JobMasterRetrySettings(null, _ => JobRetryDecision.UseRetryDelays(), null).InterceptorDescription.Should().Be("Delegate");
        new JobMasterRetrySettings(null, null, typeof(NoOpInterceptor)).InterceptorDescription.Should().Be(typeof(NoOpInterceptor).FullName);
    }

    [Theory]
    [InlineData((int)JobRetryResolution.RetryDelays, "Retrying in 00:02:00 (RetryDelays).")]
    [InlineData((int)JobRetryResolution.InterceptorUsedRetryDelays, "Retrying in 00:02:00 (RetryDelays, chosen by retry interceptor MyInterceptor).")]
    [InlineData((int)JobRetryResolution.InterceptorCustomDelay, "Retrying in 00:02:00 (retry interceptor MyInterceptor).")]
    [InlineData((int)JobRetryResolution.InterceptorBelowMinimum, "Retrying in 00:02:00 (retry interceptor MyInterceptor, raised to the 00:00:30 minimum).")]
    [InlineData((int)JobRetryResolution.InterceptorFailed, "Retrying in 00:02:00 (RetryDelays; retry interceptor MyInterceptor failed).")]
    public void RetryingLog_SaysTheDelayAndWhereItCameFrom(int resolution, string expected)
    {
        // Ends every failed execution's message: per attempt, what this worker actually applied.
        var settings = new JobMasterRetrySettings(null, null, typeof(MyInterceptor));

        settings.RetryingLog(TimeSpan.FromMinutes(2), (JobRetryResolution)resolution)
            .Should().Be(expected.Replace("MyInterceptor", typeof(MyInterceptor).FullName));
    }

    [Fact]
    public void StoppedEarlyLog_And_MaxRetriesReachedLog()
    {
        var withDelegate = new JobMasterRetrySettings(null, _ => JobRetryDecision.UseRetryDelays(), null);

        withDelegate.StoppedEarlyLog("invalid input").Should().Be("Stopped early by retry interceptor Delegate: invalid input");
        withDelegate.StoppedEarlyLog(null).Should().Be("Stopped early by retry interceptor Delegate.");
        withDelegate.MaxRetriesReachedLog(3).Should().Be("Reached max retries (3).");
    }

    [Fact]
    public void ReplacedStoredSettingsLog_OnlyWhenTheStoredSettingsDiffer()
    {
        var settings = new JobMasterRetrySettings(new[] { TimeSpan.FromMinutes(1) }, null, typeof(MyInterceptor));

        // Same as stored: a restart with the same configuration stays quiet.
        settings.ReplacedStoredSettingsLog(new List<TimeSpan> { TimeSpan.FromMinutes(1) }, typeof(MyInterceptor).FullName!)
            .Should().BeNull();

        // Different delays or interceptor: says what changed.
        settings.ReplacedStoredSettingsLog(JobMasterDefaults.RetryDelays.ToList(), "None")
            .Should().Contain("[00:00:30, 00:02:00, 00:10:00, 00:30:00, 01:00:00] / retry interceptor None")
            .And.Contain($"[00:01:00] / retry interceptor {typeof(MyInterceptor).FullName}");
        settings.ReplacedStoredSettingsLog(new List<TimeSpan> { TimeSpan.FromMinutes(1) }, "Delegate")
            .Should().NotBeNull();
    }

    [Fact]
    public void ClusterConfigurationModel_RetrySettings_RoundTripThroughItsStoredRecord()
    {
        var model = new ClusterConfigurationModel("c1")
        {
            RetryDelays = new List<TimeSpan> { TimeSpan.FromSeconds(30), TimeSpan.FromHours(1) },
            RetryInterceptor = "MyApp.MyInterceptor",
        };

        var record = InMemoryRecord(model);

        var restored = record.ToObject<ClusterConfigurationModel>();

        restored.RetryDelays.Should().Equal(TimeSpan.FromSeconds(30), TimeSpan.FromHours(1));
        restored.RetryInterceptor.Should().Be("MyApp.MyInterceptor");
    }

    [Fact]
    public void ClusterConfigurationModel_StoredBeforeRetrySettings_ReadsTheBuiltInDefaults()
    {
        // A configuration saved by an earlier version has neither field.
        var old = InMemoryRecord(new ClusterConfigurationModel("c1"));
        old.Values.Remove(nameof(ClusterConfigurationModel.RetryDelays));
        old.Values.Remove(nameof(ClusterConfigurationModel.RetryInterceptor));

        var restored = old.ToObject<ClusterConfigurationModel>();

        restored.RetryDelays.Should().Equal(JobMasterDefaults.RetryDelays);
        restored.RetryInterceptor.Should().Be("None");
    }

    // The stored record without a database. AdditionalConfig is left out: in memory it stays a dictionary object,
    // which only round-trips once a provider serializes the values (covered by the generic-record RepoConformance
    // test Upsert_ShouldRoundTrip_ClusterConfigurationRetrySettings, on every provider).
    private static GenericRecordEntry InMemoryRecord(ClusterConfigurationModel model)
    {
        var record = GenericRecordEntry.Create("c1", MasterGenericRecordGroupIds.ClusterConfiguration, "c1", model);
        record.Values.Remove(nameof(ClusterConfigurationModel.AdditionalConfig));
        return record;
    }

    private sealed class MyInterceptor : IJobMasterRetryInterceptor
    {
        public Task<JobRetryDecision> DecideAsync(JobRetryContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(JobRetryDecision.UseRetryDelays());
        }
    }

    private sealed class NoOpInterceptor : IJobMasterRetryInterceptor
    {
        public Task<JobRetryDecision> DecideAsync(JobRetryContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(JobRetryDecision.UseRetryDelays());
        }
    }
}
