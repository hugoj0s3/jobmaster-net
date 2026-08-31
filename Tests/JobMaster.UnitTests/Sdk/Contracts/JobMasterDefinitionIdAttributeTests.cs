using FluentAssertions;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;

namespace JobMaster.UnitTests.Sdk.Contracts;

public class JobMasterDefinitionIdAttributeTests
{
    [Fact]
    public void GetJobDefinitionId_WhenJobDefinitionConfigAttributePresent_UsesConfigId()
    {
        JobMasterDefinitionIdAttribute.GetJobDefinitionId(typeof(HandlerWithConfigAttribute))
            .Should().Be("config-defid");
    }

    [Fact]
    public void GetJobDefinitionId_WhenBothAttributesPresent_ConfigAttributeWins()
    {
        JobMasterDefinitionIdAttribute.GetJobDefinitionId(typeof(HandlerWithBothAttributes))
            .Should().Be("config-defid-2");
    }

    [Fact]
    public void GetJobHandlerTypeFromId_WhenJobDefinitionConfigAttributePresent_ResolvesHandler()
    {
        JobMasterDefinitionIdAttribute.GetJobHandlerTypeFromId("config-defid")
            .Should().Be(typeof(HandlerWithConfigAttribute));
    }

    private class FakeDefinitionAttribute : JobDefinitionConfigAttribute
    {
        public override JobDefinitionConfig Config { get; } = new JobDefinitionConfig("config-defid");
    }

    private class FakeDefinitionAttribute2 : JobDefinitionConfigAttribute
    {
        public override JobDefinitionConfig Config { get; } = new JobDefinitionConfig("config-defid-2");
    }

    [FakeDefinitionAttribute]
    private class HandlerWithConfigAttribute : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    [FakeDefinitionAttribute2]
    [JobMasterDefinitionId("classic-defid")]
    private class HandlerWithBothAttributes : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }
}
