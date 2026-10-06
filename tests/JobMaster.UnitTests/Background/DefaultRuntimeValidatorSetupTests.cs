using FluentAssertions;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
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
