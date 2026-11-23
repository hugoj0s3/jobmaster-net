using FluentAssertions;
using JobMaster.Postgres;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Background;
using JobMaster.Sdk.Ioc.Setup;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.UnitTests.Ioc;

public class StandaloneArchiveTests
{
    // An archived cluster's workers act as its Coordinator, which must have no agent connection; the standalone
    // AddWorker binds every worker to the standalone connection, so Archive() on a standalone cluster never started.
    [Theory]
    [InlineData(null)]
    [InlineData("target-cluster")]
    public void Finish_ArchivedStandalone_HasNoAgentConnection_AndPassesWorkerChecks(string? target)
    {
        var clusterId = "SaArch-" + Guid.NewGuid().ToString("N")[..8];
        var builder = new ClusterConfigBuilder(clusterId, new ServiceCollection());
        builder.UseStandaloneCluster()
               .UsePostgres("Host=localhost;Database=master;Username=u;Password=p")
               .Archive(target)
               .AddWorker();
        builder.Finish();

        var worker = builder.clusterDefinition.Workers.Single();
        worker.AgentConnectionName.Should().BeEmpty();
        JobMasterRuntime.ActsAsCoordinator(worker, builder.clusterDefinition).Should().BeTrue();
        DefaultRuntimeValidatorSetup.ValidateWorkersHaveAgentConnection(new[] { builder.clusterDefinition }).Should().BeNull();

        JobMasterClusterConnectionConfig.Get(clusterId, includeNotReady: true).GetAllAgentConnectionConfigs()
            .Should().NotContain(c => c.Id.EndsWith(JobMasterConstants.StandaloneAgentConnName));
    }

    [Fact]
    public void Finish_ActiveStandalone_KeepsItsAgentConnection()
    {
        var clusterId = "SaAct-" + Guid.NewGuid().ToString("N")[..8];
        var builder = new ClusterConfigBuilder(clusterId, new ServiceCollection());
        builder.UseStandaloneCluster()
               .UsePostgres("Host=localhost;Database=master;Username=u;Password=p")
               .AddWorker();
        builder.Finish();

        builder.clusterDefinition.Workers.Single().AgentConnectionName.Should().Be(JobMasterConstants.StandaloneAgentConnName);
        JobMasterClusterConnectionConfig.Get(clusterId, includeNotReady: true).GetAllAgentConnectionConfigs()
            .Should().Contain(c => c.Id.EndsWith(JobMasterConstants.StandaloneAgentConnName));
    }
}
