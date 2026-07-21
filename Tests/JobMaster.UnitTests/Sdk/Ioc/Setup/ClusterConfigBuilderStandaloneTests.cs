using FluentAssertions;
using JobMaster.Sdk.Ioc.Setup;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace JobMaster.UnitTests.Sdk.Ioc.Setup;

/// <summary>
/// Regression coverage for a real bug: ClusterDefinition.IsStandalone is nullable specifically so
/// JobMasterRuntime.StartAsync can tell "this config didn't mention it" (defer to whatever was
/// last persisted) apart from "this config explicitly says non-standalone". Before the fix,
/// ApplyJsonConfig only ever set IsStandalone when the JSON said Standalone:true -- the non-standalone
/// branch never set it to false, leaving it null and silently deferring to a stale persisted `true`
/// forever. That made it impossible to ever move a cluster off Standalone via config, which in turn
/// meant JobMasterRuntime's StandaloneDrainer auto-synthesis (which only runs when isStandalone is
/// false) could never fire -- jobs stuck on the dead standalone worker's buckets had no recovery path.
/// </summary>
public class ClusterConfigBuilderStandaloneTests
{
    private static ClusterConfigBuilder NewBuilder() => new(clusterId: null, new ServiceCollection());

    [Fact]
    public void ConfigFromJson_SetsIsStandaloneFalse_WhenStandaloneFieldIsOmitted()
    {
        var builder = NewBuilder();

        builder.ConfigFromJson("""{ "ClusterId": "my-cluster" }""");

        builder.clusterDefinition.IsStandalone.Should().BeFalse(
            "an explicit false must be set so JobMasterRuntime's isStandalone = clusterDefinition.IsStandalone ?? modelToSave.IsStandalone " +
            "does not fall back to a stale persisted true from an earlier standalone run");
    }

    [Fact]
    public void ConfigFromJson_SetsIsStandaloneFalse_WhenStandaloneFieldIsExplicitlyFalse()
    {
        var builder = NewBuilder();

        builder.ConfigFromJson("""{ "ClusterId": "my-cluster", "Standalone": false }""");

        builder.clusterDefinition.IsStandalone.Should().BeFalse();
    }

    [Fact]
    public void ConfigFromJson_SetsIsStandaloneTrue_WhenStandaloneFieldIsTrue()
    {
        var builder = NewBuilder();

        builder.ConfigFromJson("""{ "ClusterId": "my-cluster", "Standalone": true, "Workers": [ { "WorkerName": "w1" } ] }""");

        builder.clusterDefinition.IsStandalone.Should().BeTrue();
    }
}
