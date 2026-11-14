using FluentAssertions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.SqlBase;

namespace JobMaster.UnitTests.Ioc;

public class SqlAutoProvisionSelectionTests
{
    // Each test uses its own repo type, so clusters registered by other tests in the global config list are never selected.
    private readonly string repoType = $"sql-test-{Guid.NewGuid():N}";
    private const string OtherRepoType = "other-repo-type";

    [Fact]
    public void GetClustersToProvision_OptedOutCluster_SkipsOnlyThatCluster()
    {
        var enabledA = NewCluster(repoType);
        var optedOut = NewCluster(repoType, disableAutoProvision: true);
        var enabledB = NewCluster(repoType);
        NewCluster(OtherRepoType);

        var selected = new TestSqlRuntimeSetup(repoType).GetClustersToProvision();

        selected.Select(c => c.ClusterId).Should().BeEquivalentTo(enabledA.ClusterId, enabledB.ClusterId);
        selected.Should().NotContain(optedOut);
    }

    [Fact]
    public void GetAgentConfigsToProvision_OptedOutCluster_SkipsOnlyThatClustersAgents()
    {
        var enabled = NewCluster(repoType);
        enabled.AddAgentConnectionString("single", "conn-single", repoType);
        enabled.AddHybridAgentConnectionConfig("hybrid", "conn-transport", repoType, null, "conn-execution", OtherRepoType, null);

        var optedOut = NewCluster(repoType, disableAutoProvision: true);
        optedOut.AddAgentConnectionString("single", "conn-opted-out", repoType);

        var selected = new TestSqlRuntimeSetup(repoType).GetAgentConfigsToProvision();

        // The hybrid's execution leg is on another provider, so only its transport leg is selected.
        selected.Select(x => x.AgentConfig.ConnectionString).Should().BeEquivalentTo("conn-single", "conn-transport");
        selected.Should().OnlyContain(x => ReferenceEquals(x.ClusterConfig, enabled));
    }

    private static JobMasterClusterConnectionConfig NewCluster(string repositoryTypeId, bool disableAutoProvision = false)
    {
        var cluster = JobMasterClusterConnectionConfig.Create($"prov-{Guid.NewGuid():N}", repositoryTypeId, "conn-master");
        if (disableAutoProvision)
        {
            cluster.AdditionalConnConfig.SetValue(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.DisableAutoProvisionSchemaKey, true);
        }

        return cluster;
    }

    private sealed class TestSqlRuntimeSetup : SqlJobMasterRuntimeSetup
    {
        public TestSqlRuntimeSetup(string repositoryTypeId)
        {
            RepositoryTypeId = repositoryTypeId;
        }

        public override string RepositoryTypeId { get; }
    }
}
