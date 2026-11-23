using FluentAssertions;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Sdk.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.UnitTests.Sdk.Reflection;

public class JobDefinitionIdResolverTests
{
    [Fact]
    public void GetJobDefinitionId_WhenJobDefinitionConfigAttributePresent_UsesConfigId()
    {
        typeof(HandlerWithConfigAttribute).GetJobDefinitionId()
            .Should().Be("config-defid");
    }

    [Fact]
    public void GetJobDefinitionId_WhenBothAttributesPresent_ConfigAttributeWins()
    {
        typeof(HandlerWithBothAttributes).GetJobDefinitionId()
            .Should().Be("config-defid-2");
    }

    [Fact]
    public void GetJobHandlerTypeFromId_WhenJobDefinitionConfigAttributePresent_ResolvesHandler()
    {
        JobDefinitionIdResolver.GetJobHandlerTypeFromId("config-defid")
            .Should().Be(typeof(HandlerWithConfigAttribute));
    }

    [Fact]
    public void GetJobHandlerTypeFromId_ResolvesByDefinitionIdAttribute()
    {
        JobDefinitionIdResolver.GetJobHandlerTypeFromId("classic-only-defid")
            .Should().Be(typeof(HandlerWithDefinitionIdAttribute));
    }

    [Fact]
    public void GetJobHandlerTypeFromId_ResolvesByFullName()
    {
        JobDefinitionIdResolver.GetJobHandlerTypeFromId(typeof(HandlerWithoutAttributes).FullName!)
            .Should().Be(typeof(HandlerWithoutAttributes));
    }

    [Fact]
    public void GetJobHandlerTypeFromId_WhenUnknown_ReturnsNull_EveryTime()
    {
        JobDefinitionIdResolver.GetJobHandlerTypeFromId("unknown-defid").Should().BeNull();
        JobDefinitionIdResolver.GetJobHandlerTypeFromId("unknown-defid").Should().BeNull();
    }

    [Fact]
    public void GetJobHandler_WhenRegistered_ReturnsHandler()
    {
        var serviceProvider = new ServiceCollection().AddScoped<HandlerWithDefinitionIdAttribute>().BuildServiceProvider();

        serviceProvider.GetJobHandler("classic-only-defid").Should().BeOfType<HandlerWithDefinitionIdAttribute>();
    }

    [Fact]
    public void GetJobHandler_WhenIdUnknown_Throws()
    {
        var serviceProvider = new ServiceCollection().BuildServiceProvider();

        var act = () => serviceProvider.GetJobHandler("unknown-defid");

        act.Should().Throw<InvalidOperationException>().WithMessage("*not found*unknown-defid*");
    }

    [Fact]
    public void GetJobHandler_WhenNotRegistered_Throws()
    {
        var serviceProvider = new ServiceCollection().BuildServiceProvider();

        var act = () => serviceProvider.GetJobHandler("classic-only-defid");

        act.Should().Throw<InvalidOperationException>().WithMessage("*not registered in the DI container*");
    }

    [JobMasterDefinitionId("classic-only-defid")]
    private class HandlerWithDefinitionIdAttribute : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    private class HandlerWithoutAttributes : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    private class FakeDefinitionAttribute : JobDefinitionConfigAttribute, IStaticJobDefinitionConfig
    {
        public static JobDefinitionConfig Config { get; } = new JobDefinitionConfig("config-defid");
    }

    private class FakeDefinitionAttribute2 : JobDefinitionConfigAttribute, IStaticJobDefinitionConfig
    {
        public static JobDefinitionConfig Config { get; } = new JobDefinitionConfig("config-defid-2");
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
