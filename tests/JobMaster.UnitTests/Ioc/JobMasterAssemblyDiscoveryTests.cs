using System.Reflection;
using System.Reflection.Emit;
using FluentAssertions;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Ioc.Extensions;
using JobMaster.Sdk.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.UnitTests.Ioc;

// Uses this test assembly (already loaded) rather than a fake one: the added set is process-wide, and a fake assembly
// would show its types twice to every other test's scans.
public class JobMasterAssemblyDiscoveryTests
{
    [Fact]
    public void AddAssemblyContaining_AddsTheAssemblyToDiscovery()
    {
        new ServiceCollection().JobMasterAssemblyDiscovery().AddAssemblyContaining<DiscoveredHandler>();

        TypeDiscovery.GetLoadedAssemblies().Should().Contain(typeof(DiscoveredHandler).Assembly)
            .And.OnlyHaveUniqueItems();
    }

    [Fact]
    public void AddAssemblyContaining_RegistersTheAssemblyHandlersInDi()
    {
        var services = new ServiceCollection();

        services.JobMasterAssemblyDiscovery().AddAssemblyContaining<DiscoveredHandler>();

        using var serviceProvider = services.BuildServiceProvider();
        using var scope = serviceProvider.CreateScope();
        scope.ServiceProvider.GetJobHandler(typeof(DiscoveredHandler).GetJobDefinitionId())
            .Should().BeOfType<DiscoveredHandler>();
        services.Should().ContainSingle(d => d.ServiceType == typeof(DiscoveredHandler))
            .Which.Lifetime.Should().Be(ServiceLifetime.Scoped);
    }

    [Fact]
    public void AddAssemblyContaining_KeepsAnExistingRegistration()
    {
        var services = new ServiceCollection();
        services.AddSingleton<DiscoveredHandler>();

        services.JobMasterAssemblyDiscovery()
            .AddAssemblyContaining<DiscoveredHandler>()
            .AddAssembly(typeof(DiscoveredHandler).Assembly);

        services.Should().ContainSingle(d => d.ServiceType == typeof(DiscoveredHandler))
            .Which.Lifetime.Should().Be(ServiceLifetime.Singleton);
    }

    [Fact]
    public void AddAssembly_WhenDynamic_Throws()
    {
        var dynamicAssembly = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("JobMasterDynamicTest"), AssemblyBuilderAccess.Run);

        var act = () => new ServiceCollection().JobMasterAssemblyDiscovery().AddAssembly(dynamicAssembly);

        act.Should().Throw<ArgumentException>().WithMessage("*dynamic*");
        TypeDiscovery.GetLoadedAssemblies().Should().NotContain(dynamicAssembly);
    }

    [Fact]
    public void AddAssemblies_WhenNull_Throws()
    {
        var act = () => new ServiceCollection().JobMasterAssemblyDiscovery().AddAssemblies(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void JobMasterAssemblyDiscovery_WhenServicesNull_Throws()
    {
        var act = () => ((IServiceCollection)null!).JobMasterAssemblyDiscovery();

        act.Should().Throw<ArgumentNullException>();
    }

    private class DiscoveredHandler : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }
}
