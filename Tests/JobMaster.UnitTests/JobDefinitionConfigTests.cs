using FluentAssertions;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;

namespace JobMaster.UnitTests;

public class JobDefinitionConfigTests
{
    [Fact]
    public void Constructor_WhenJobDefinitionIdIsNull_Throws()
    {
        var act = () => new JobDefinitionConfig(null!);
        act.Should().Throw<ArgumentException>().WithParameterName("jobDefinitionId");
    }

    [Fact]
    public void Constructor_WhenJobDefinitionIdIsEmpty_Throws()
    {
        var act = () => new JobDefinitionConfig(string.Empty);
        act.Should().Throw<ArgumentException>().WithParameterName("jobDefinitionId");
    }

    [Fact]
    public void Constructor_WhenJobDefinitionIdIsWhitespace_Throws()
    {
        var act = () => new JobDefinitionConfig("   ");
        act.Should().Throw<ArgumentException>().WithParameterName("jobDefinitionId");
    }

    [Fact]
    public void Constructor_WhenOnlyJobDefinitionIdProvided_LeavesRestNull()
    {
        var config = new JobDefinitionConfig("orders.process");

        config.JobDefinitionId.Should().Be("orders.process");
        config.Priority.Should().BeNull();
        config.Timeout.Should().BeNull();
        config.MaxNumberOfRetries.Should().BeNull();
        config.WorkerLane.Should().BeNull();
        config.Metadata.Should().BeNull();
    }

    [Fact]
    public void Constructor_WhenAllValuesProvided_SetsThemAll()
    {
        var config = new JobDefinitionConfig(
            "orders.process",
            priority: JobMasterPriority.High,
            timeout: TimeSpan.FromSeconds(60),
            maxNumberOfRetries: 5,
            workerLane: "orders");

        config.JobDefinitionId.Should().Be("orders.process");
        config.Priority.Should().Be(JobMasterPriority.High);
        config.Timeout.Should().Be(TimeSpan.FromSeconds(60));
        config.MaxNumberOfRetries.Should().Be(5);
        config.WorkerLane.Should().Be("orders");
    }
}
