using FluentAssertions;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Background;

namespace JobMaster.UnitTests.Background;

public class DefaultRuntimeValidatorSetupTests
{
    [Fact]
    public void ValidateClusterIds_WhenAttributeClusterNotConfigured_ReturnsError()
    {
        var error = DefaultRuntimeValidatorSetup.ValidateClusterIds(
            new[] { typeof(AttrClusterHandler) },
            Type.EmptyTypes,
            new[] { "default" });

        error.Should().NotBeNull();
        error.Should().Contain(typeof(AttrClusterHandler).FullName).And.Contain("'attr-cluster'");
    }

    [Fact]
    public void ValidateClusterIds_WhenAppliedDefinitionConfigClusterNotConfigured_ReturnsError()
    {
        var error = DefaultRuntimeValidatorSetup.ValidateClusterIds(
            new[] { typeof(ConfigClusterHandler) },
            Type.EmptyTypes,
            new[] { "default" });

        error.Should().NotBeNull();
        error.Should().Contain("'config-cluster'");
    }

    [Fact]
    public void ValidateClusterIds_WhenDefinitionOnlyConfigClusterNotConfigured_ReturnsError()
    {
        // Publisher side: the definition type is never applied to a handler in this app.
        var error = DefaultRuntimeValidatorSetup.ValidateClusterIds(
            Type.EmptyTypes,
            new[] { typeof(PublisherOnlyDefinitionAttribute) },
            new[] { "default" });

        error.Should().NotBeNull();
        error.Should().Contain(typeof(PublisherOnlyDefinitionAttribute).FullName).And.Contain("'publisher-cluster'");
    }

    [Fact]
    public void ValidateClusterIds_WhenDefinitionHasNoClusterId_ReturnsNull()
    {
        DefaultRuntimeValidatorSetup.ValidateClusterIds(
                Type.EmptyTypes,
                new[] { typeof(NoClusterDefinitionAttribute) },
                new[] { "default" })
            .Should().BeNull();
    }

    [Fact]
    public void ValidateClusterIds_WhenClusterConfigured_IgnoringCase_ReturnsNull()
    {
        DefaultRuntimeValidatorSetup.ValidateClusterIds(
                new[] { typeof(AttrClusterHandler), typeof(ConfigClusterHandler), typeof(PlainHandler) },
                new[] { typeof(ConfigClusterDefinitionAttribute), typeof(PublisherOnlyDefinitionAttribute) },
                new[] { "default", "ATTR-CLUSTER", "config-cluster", "publisher-cluster" })
            .Should().BeNull();
    }

    [Fact]
    public void ValidateClusterIds_WhenHandlerDeclaresNoCluster_ReturnsNull()
    {
        DefaultRuntimeValidatorSetup.ValidateClusterIds(new[] { typeof(PlainHandler) }, Type.EmptyTypes, new[] { "default" })
            .Should().BeNull();
    }

    [Fact]
    public void ValidateStaticProfileClusterIds_WhenProfileClusterNotConfigured_ReturnsError()
    {
        var error = DefaultRuntimeValidatorSetup.ValidateStaticProfileClusterIds(
            new[] { typeof(UnknownClusterProfile) },
            new[] { "default" });

        error.Should().NotBeNull();
        error.Should().Contain(typeof(UnknownClusterProfile).FullName).And.Contain("'profile-cluster'");
    }

    [Fact]
    public void ValidateStaticProfileClusterIds_WhenProfileClusterConfigured_IgnoringCase_ReturnsNull()
    {
        DefaultRuntimeValidatorSetup.ValidateStaticProfileClusterIds(
                new[] { typeof(UnknownClusterProfile) },
                new[] { "default", "PROFILE-CLUSTER" })
            .Should().BeNull();
    }

    [Fact]
    public void ValidateStaticProfileClusterIds_WhenProfileDeclaresNoCluster_ReturnsNull()
    {
        DefaultRuntimeValidatorSetup.ValidateStaticProfileClusterIds(
                new[] { typeof(NoClusterProfile) },
                new[] { "default" })
            .Should().BeNull();
    }

    [Fact]
    public void ValidateSingleDefinitionAttribute_WhenHandlerAppliesTwo_ReturnsError()
    {
        var error = DefaultRuntimeValidatorSetup.ValidateSingleDefinitionAttribute(new[] { typeof(TwoDefinitionsHandler) });

        error.Should().NotBeNull();
        error.Should().Contain(typeof(TwoDefinitionsHandler).FullName);
    }

    [Fact]
    public void ValidateSingleDefinitionAttribute_WhenAtMostOne_ReturnsNull()
    {
        DefaultRuntimeValidatorSetup.ValidateSingleDefinitionAttribute(new[] { typeof(ConfigClusterHandler), typeof(PlainHandler) })
            .Should().BeNull();
    }

    [Fact]
    public void ValidateDisabledPriorities_WhenDisabledOnlyOnAnotherCluster_ReturnsNull()
    {
        // Critical handler on the default cluster; only cluster-b disables Critical.
        DefaultRuntimeValidatorSetup.ValidateDisabledPriorities(
                new[] { typeof(CriticalHandler) },
                Type.EmptyTypes,
                Clusters(("default", false), ("cluster-b", true)),
                defaultClusterId: "default")
            .Should().BeNull();
    }

    [Fact]
    public void ValidateDisabledPriorities_WhenDisabledOnHandlerCluster_ReturnsError()
    {
        var error = DefaultRuntimeValidatorSetup.ValidateDisabledPriorities(
            new[] { typeof(CriticalClusterBHandler) },
            Type.EmptyTypes,
            Clusters(("default", false), ("CLUSTER-B", true)),
            defaultClusterId: "default");

        error.Should().NotBeNull();
        error.Should().Contain(typeof(CriticalClusterBHandler).FullName).And.Contain("Critical on 'CLUSTER-B'");
    }

    [Fact]
    public void ValidateDisabledPriorities_WhenHandlerHasNoCluster_UsesDefaultCluster()
    {
        DefaultRuntimeValidatorSetup.ValidateDisabledPriorities(
                new[] { typeof(CriticalHandler) },
                Type.EmptyTypes,
                Clusters(("default", true)),
                defaultClusterId: "default")
            .Should().Contain(typeof(CriticalHandler).FullName);
    }

    [Fact]
    public void ValidateDisabledPriorities_ChecksDefinitionConfigPriority()
    {
        DefaultRuntimeValidatorSetup.ValidateDisabledPriorities(
                new[] { typeof(CriticalConfigHandler) },
                Type.EmptyTypes,
                Clusters(("default", false), ("cluster-b", true)),
                defaultClusterId: "default")
            .Should().Contain(typeof(CriticalConfigHandler).FullName);
    }

    [Fact]
    public void ValidateDisabledPriorities_ChecksInheritedPriorityAttribute()
    {
        DefaultRuntimeValidatorSetup.ValidateDisabledPriorities(
                new[] { typeof(InheritedCriticalHandler) },
                Type.EmptyTypes,
                Clusters(("default", true)),
                defaultClusterId: "default")
            .Should().Contain(typeof(InheritedCriticalHandler).FullName);
    }

    [Fact]
    public void ValidateDisabledPriorities_ChecksDefinitionOnlyConfig()
    {
        // Publisher side: the definition type is never applied to a handler in this app.
        var error = DefaultRuntimeValidatorSetup.ValidateDisabledPriorities(
            Type.EmptyTypes,
            new[] { typeof(CriticalClusterBDefinitionAttribute) },
            Clusters(("default", false), ("cluster-b", true)),
            defaultClusterId: "default");

        error.Should().Contain(typeof(CriticalClusterBDefinitionAttribute).FullName);
    }

    [Fact]
    public void ValidateDisabledPriorities_WhenClusterUnknown_ReturnsNull()
    {
        // Reported by ValidateClusterIds instead.
        DefaultRuntimeValidatorSetup.ValidateDisabledPriorities(
                new[] { typeof(CriticalClusterBHandler) },
                Type.EmptyTypes,
                Clusters(("default", true)),
                defaultClusterId: "default")
            .Should().BeNull();
    }

    // Each cluster disables Critical or nothing.
    private static IEnumerable<ClusterDefinition> Clusters(params (string ClusterId, bool DisablesCritical)[] clusters) =>
        clusters.Select(c =>
        {
            var cluster = new ClusterDefinition { ClusterId = c.ClusterId };
            if (c.DisablesCritical)
            {
                cluster.DisablePriority(JobMasterPriority.Critical);
            }

            return cluster;
        }).ToList();

    [JobMasterPriority(JobMasterPriority.Critical)]
    private sealed class CriticalHandler : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    [JobMasterPriority(JobMasterPriority.Critical)]
    [JobMasterClusterId("cluster-b")]
    private sealed class CriticalClusterBHandler : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    private sealed class CriticalClusterBDefinitionAttribute : JobDefinitionConfigAttribute, IStaticJobDefinitionConfig
    {
        public static JobDefinitionConfig Config { get; } =
            new JobDefinitionConfig("validator-critical-defid", priority: JobMasterPriority.Critical, clusterId: "cluster-b");
    }

    [CriticalClusterBDefinitionAttribute]
    private sealed class CriticalConfigHandler : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    [JobMasterPriority(JobMasterPriority.Critical)]
    private abstract class CriticalBaseHandler : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    private sealed class InheritedCriticalHandler : CriticalBaseHandler;

    [ConfigClusterDefinitionAttribute]
    [NoClusterDefinitionAttribute]
    private sealed class TwoDefinitionsHandler : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    // Plain types with the same static members the validator reads — deliberately not implementing
    // IStaticRecurringSchedulesProfile, so the runtime's assembly-wide profile discovery never picks them up.
    private sealed class UnknownClusterProfile
    {
        public static string ProfileId => "validator-profile";
        public static string ClusterId => "profile-cluster";
    }

    private sealed class NoClusterProfile
    {
        public static string ProfileId => "validator-profile-no-cluster";
    }

    private sealed class PlainHandler : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    [JobMasterClusterId("attr-cluster")]
    private sealed class AttrClusterHandler : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    private sealed class ConfigClusterDefinitionAttribute : JobDefinitionConfigAttribute, IStaticJobDefinitionConfig
    {
        public static JobDefinitionConfig Config { get; } = new JobDefinitionConfig("validator-defid", clusterId: "config-cluster");
    }

    [ConfigClusterDefinitionAttribute]
    private sealed class ConfigClusterHandler : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    private sealed class PublisherOnlyDefinitionAttribute : JobDefinitionConfigAttribute, IStaticJobDefinitionConfig
    {
        public static JobDefinitionConfig Config { get; } = new JobDefinitionConfig("validator-publisher-defid", clusterId: "publisher-cluster");
    }

    private sealed class NoClusterDefinitionAttribute : JobDefinitionConfigAttribute, IStaticJobDefinitionConfig
    {
        public static JobDefinitionConfig Config { get; } = new JobDefinitionConfig("validator-noclusters-defid");
    }
}
