using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;

namespace JobMaster.UnitTests.Sdk.Background;

public class ClusterDefinitionArchiveTests
{
    [Fact]
    public void Archive_WithTarget_SetsArchivedAndTarget()
    {
        var definition = new ClusterDefinition();
        definition.Archive("c2");

        definition.ClusterMode.Should().Be(ClusterMode.Archived);
        definition.TargetActiveClusterId.Should().Be("c2");
    }

    [Fact]
    public void Archive_WithoutTarget_SetsArchived_KeepsConfiguredTarget()
    {
        var definition = new ClusterDefinition { TargetActiveClusterId = "c2" };
        definition.Archive(null);

        definition.ClusterMode.Should().Be(ClusterMode.Archived);
        definition.TargetActiveClusterId.Should().Be("c2");
    }

    [Theory]
    [InlineData(null)]
    [InlineData(ClusterMode.Archived)]
    public void MigrateTo_AlwaysSwitchesToMigrating(ClusterMode? mode)
    {
        var definition = new ClusterDefinition { ClusterMode = mode };
        definition.MigrateTo("c2");

        definition.ClusterMode.Should().Be(ClusterMode.Migrating);
    }
}
