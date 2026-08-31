using FluentAssertions;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;

namespace JobMaster.UnitTests.Abstractions;

public class JobDefinitionConfigAttributeTests
{
    [Fact]
    public void GetConfig_WhenDefinitionTypeHasNoStaticConfig_ThrowsInvalidOperationException()
    {
        var act = () => JobDefinitionConfigAttribute.GetConfig(typeof(DefinitionMissingConfigAttribute));

        act.Should().Throw<InvalidOperationException>()
            .WithMessage($"*{typeof(DefinitionMissingConfigAttribute).FullName}*");
    }

    [Fact]
    public void TryGetAppliedConfig_WhenAppliedAttributeHasNoStaticConfig_ThrowsInvalidOperationException()
    {
        var act = () => JobDefinitionConfigAttribute.TryGetAppliedConfig(typeof(HandlerWithBrokenDefinitionAttribute));

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void TryGetAppliedConfig_WhenNoAttributeApplied_ReturnsNull()
    {
        JobDefinitionConfigAttribute.TryGetAppliedConfig(typeof(PlainHandler)).Should().BeNull();
    }

    [Fact]
    public void TryGetConfig_WhenDefinitionTypeHasStaticConfig_ReturnsTrueAndConfig()
    {
        var result = JobDefinitionConfigAttribute.TryGetConfig(typeof(ValidDefinitionAttribute), out var config);

        result.Should().BeTrue();
        config.Should().BeSameAs(ValidDefinitionAttribute.Config);
    }

    [Fact]
    public void TryGetConfig_WhenDefinitionTypeHasNoStaticConfig_ReturnsFalse()
    {
        var result = JobDefinitionConfigAttribute.TryGetConfig(typeof(DefinitionMissingConfigAttribute), out var config);

        result.Should().BeFalse();
        config.Should().BeNull();
    }

    private class ValidDefinitionAttribute : JobDefinitionConfigAttribute, IStaticJobDefinitionConfig
    {
        public static JobDefinitionConfig Config { get; } = new JobDefinitionConfig("valid-defid");
    }

    // Deliberately doesn't implement IStaticJobDefinitionConfig / declare Config -- simulates a subclass
    // that forgot to (the case IStaticJobDefinitionConfig only catches at compile time on net8.0, and only
    // when the type is actually used as a TDefinition generic argument).
    private class DefinitionMissingConfigAttribute : JobDefinitionConfigAttribute
    {
    }

    // Deliberately NOT an IJobMasterHandler -- JobMasterDefinitionIdAttribute.GetJobHandlerTypeFromId does
    // an AppDomain-wide scan over every loaded IJobMasterHandler type and evaluates TryGetAppliedConfig on
    // each one unconditionally; a broken definition attribute on an actual IJobMasterHandler here would
    // make that throw for every other test in the process that resolves a handler by JobDefinitionId, not
    // just this one.
    [DefinitionMissingConfigAttribute]
    private class HandlerWithBrokenDefinitionAttribute
    {
    }

    private class PlainHandler : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }
}
