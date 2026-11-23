using System.Reflection;
using FluentAssertions;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Ioc.Setup;
using JobMaster.Sdk.Reflection;
using JobMaster.Sdk.Utils;
using Moq;

namespace JobMaster.UnitTests.Sdk.Reflection;

public class ProviderAssembliesTests
{
    public static TheoryData<string, string> Providers()
    {
        var data = new TheoryData<string, string>();
        foreach (var (repositoryType, assemblyName) in ProviderAssemblies.AssemblyNameByRepositoryType)
        {
            data.Add(repositoryType, assemblyName);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void AssemblyNameByRepositoryType_MatchesTheProviderRegistration(string repositoryType, string assemblyName)
    {
        var assembly = Assembly.Load(new AssemblyName(assemblyName));

        var registeredRepositoryTypes = assembly.GetTypes()
            .Where(t => t.GetCachedAttribute<JobMasterIocRegistrationAttribute>() != null)
            .Select(t => t.GetProperty("RepositoryType", BindingFlags.Public | BindingFlags.Static)?.GetValue(null) as string);

        registeredRepositoryTypes.Should().Contain(repositoryType);
    }

    [Fact]
    public void EnsureLoaded_WhenUnknown_DoesNothing()
    {
        var act = () => ProviderAssemblies.EnsureLoaded("NotAProvider");

        act.Should().NotThrow();
    }

    [Fact]
    public void RegisterProviderExtensions_WhenNoProviderMatches_Throws()
    {
        var act = () => JobMasterIocRegistrationAttribute.RegisterProviderExtensionsForMaster(
            Mock.Of<IClusterIocRegistration>(), "NotAProvider", "c1");

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("No JobMaster provider found*'NotAProvider'*'c1'*Postgres*");
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void ToKnownRepositoryType_AnyCasing_ReturnsTheBuiltInCasing(string repositoryType, string _)
    {
        ProviderAssemblies.ToKnownRepositoryType(repositoryType.ToLowerInvariant()).Should().Be(repositoryType);
        ProviderAssemblies.ToKnownRepositoryType(repositoryType.ToUpperInvariant()).Should().Be(repositoryType);
        ProviderAssemblies.ToKnownRepositoryType(repositoryType).Should().Be(repositoryType);
    }

    [Fact]
    public void ToKnownRepositoryType_WhenNotBuiltIn_ReturnsItUnchanged()
    {
        // A third-party provider keeps its own exact name.
        ProviderAssemblies.ToKnownRepositoryType("myCustomProvider").Should().Be("myCustomProvider");
    }

    [Fact]
    public void FormatHybridRepoType_AnyCasing_UsesTheBuiltInCasing()
    {
        RepoTypeAndFingerprintUtil.FormatHybridRepoType("natsjetstream", "RAVENDB").Should().Be("hybrid:NatsJetStream&&RavenDB");
    }

    [Fact]
    public void RegisterProviderExtensions_WhenProviderHasNoMasterRegistration_Throws()
    {
        var act = () => JobMasterIocRegistrationAttribute.RegisterProviderExtensionsForMaster(
            Mock.Of<IClusterIocRegistration>(), "NatsJetStream", "c1");

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("The 'NatsJetStream' provider can't be used as master (cluster 'c1').");
    }
}
