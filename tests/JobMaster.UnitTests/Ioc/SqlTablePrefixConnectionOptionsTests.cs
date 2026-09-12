using FluentAssertions;
using JobMaster.MySql;
using JobMaster.Postgres;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Ioc.Setup;
using JobMaster.SqlBase;
using JobMaster.SqlServer;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.UnitTests.Ioc;

public class SqlTablePrefixConnectionOptionsTests
{
    static SqlTablePrefixConnectionOptionsTests()
    {
        // Force the SQL provider assemblies to load before the binder factory initializes its static discovery scan.
        _ = typeof(PostgresConnectionOptionsBinder);
        _ = typeof(MySqlConnectionOptionsBinder);
        _ = typeof(SqlServerConnectionOptionsBinder);
    }

    private static ClusterConfigBuilder Cluster() => new(null, new ServiceCollection());

    // ── fluent API: tablePrefix parameter (Postgres/MySql/SqlServer × Master/Agent/Standalone) ─────────

    [Fact]
    public void UsePostgresForMaster_WithTablePrefix_SetsPrefix()
    {
        var cluster = Cluster();

        cluster.UsePostgresForMaster("Host=localhost;", tablePrefix: "Custom_");

        cluster.clusterDefinition.AdditionalConnConfig!
            .TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey)
            .Should().Be("Custom_");
    }

    [Fact]
    public void UsePostgresForMaster_WithoutTablePrefix_DoesNotWritePrefix()
    {
        var cluster = Cluster();

        cluster.UsePostgresForMaster("Host=localhost;");

        cluster.clusterDefinition.AdditionalConnConfig.Should().BeNull();
    }

    [Fact]
    public void UsePostgresForAgent_WithTablePrefix_SetsPrefix()
    {
        var cluster = Cluster();
        var agentSel = cluster.AddAgentConnectionConfig("Agent-1", "Postgres", "Host=localhost;");

        agentSel.UsePostgresForAgent("Host=localhost;", tablePrefix: "Custom_");

        cluster.clusterDefinition.AgentConnections[0].AgentAdditionalConnConfig!
            .TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey)
            .Should().Be("Custom_");
    }

    [Fact]
    public void UsePostgres_Standalone_WithTablePrefix_SetsPrefix()
    {
        var clusterDefinition = new ClusterDefinition();
        var standalone = new ClusterStandaloneConfigBuilder(clusterDefinition);

        standalone.UsePostgres("Host=localhost;", tablePrefix: "Custom_");

        clusterDefinition.AdditionalConnConfig!
            .TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey)
            .Should().Be("Custom_");
    }

    [Fact]
    public void UseMySqlForMaster_WithTablePrefix_SetsPrefix()
    {
        var cluster = Cluster();

        cluster.UseMySqlForMaster("Server=localhost;", tablePrefix: "Custom_");

        cluster.clusterDefinition.AdditionalConnConfig!
            .TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey)
            .Should().Be("Custom_");
    }

    [Fact]
    public void UseMySqlForAgent_WithTablePrefix_SetsPrefix()
    {
        var cluster = Cluster();
        var agentSel = cluster.AddAgentConnectionConfig("Agent-1", "MySql", "Server=localhost;");

        agentSel.UseMySqlForAgent("Server=localhost;", tablePrefix: "Custom_");

        cluster.clusterDefinition.AgentConnections[0].AgentAdditionalConnConfig!
            .TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey)
            .Should().Be("Custom_");
    }

    [Fact]
    public void UseMySql_Standalone_WithTablePrefix_SetsPrefix()
    {
        var clusterDefinition = new ClusterDefinition();
        var standalone = new ClusterStandaloneConfigBuilder(clusterDefinition);

        standalone.UseMySql("Server=localhost;", tablePrefix: "Custom_");

        clusterDefinition.AdditionalConnConfig!
            .TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey)
            .Should().Be("Custom_");
    }

    [Fact]
    public void UseSqlServerForMaster_WithTablePrefix_SetsPrefix()
    {
        var cluster = Cluster();

        cluster.UseSqlServerForMaster("Server=localhost;", tablePrefix: "Custom_");

        cluster.clusterDefinition.AdditionalConnConfig!
            .TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey)
            .Should().Be("Custom_");
    }

    [Fact]
    public void UseSqlServerForAgent_WithTablePrefix_SetsPrefix()
    {
        var cluster = Cluster();
        var agentSel = cluster.AddAgentConnectionConfig("Agent-1", "SqlServer", "Server=localhost;");

        agentSel.UseSqlServerForAgent("Server=localhost;", tablePrefix: "Custom_");

        cluster.clusterDefinition.AgentConnections[0].AgentAdditionalConnConfig!
            .TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey)
            .Should().Be("Custom_");
    }

    [Fact]
    public void UseSqlServer_Standalone_WithTablePrefix_SetsPrefix()
    {
        var clusterDefinition = new ClusterDefinition();
        var standalone = new ClusterStandaloneConfigBuilder(clusterDefinition);

        standalone.UseSqlServer("Server=localhost;", tablePrefix: "Custom_");

        clusterDefinition.AdditionalConnConfig!
            .TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey)
            .Should().Be("Custom_");
    }

    // ── PostgresConnectionOptionsBinder ──────────────────────────────────────

    [Fact]
    public void PostgresBinder_SetOptions_Cluster_TablePrefix_SetsPrefix()
    {
        var cluster = Cluster();

        new PostgresConnectionOptionsBinder().SetOptions(cluster, new Dictionary<string, object> { ["tablePrefix"] = "Custom_" });

        cluster.clusterDefinition.AdditionalConnConfig!
            .TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey)
            .Should().Be("Custom_");
    }

    [Fact]
    public void PostgresBinder_SetOptions_Agent_TablePrefix_SetsPrefix()
    {
        var cluster = Cluster();
        var agentSel = cluster.AddAgentConnectionConfig("Agent-1", "Postgres", "Host=localhost;");

        new PostgresConnectionOptionsBinder().SetOptions(agentSel, new Dictionary<string, object> { ["tablePrefix"] = "Custom_" });

        cluster.clusterDefinition.AgentConnections[0].AgentAdditionalConnConfig!
            .TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey)
            .Should().Be("Custom_");
    }

    [Fact]
    public void PostgresBinder_SetOptions_UnknownKey_ThrowsArgumentException()
    {
        var cluster = Cluster();

        var act = () => new PostgresConnectionOptionsBinder().SetOptions(cluster, new Dictionary<string, object> { ["unknownProp"] = "x" });

        act.Should().Throw<ArgumentException>().WithMessage("*unknownProp*");
    }

    [Fact]
    public void PostgresBinder_SetOptions_KeyCaseInsensitive_DoesNotThrow()
    {
        var cluster = Cluster();

        var act = () => new PostgresConnectionOptionsBinder().SetOptions(
            cluster,
            new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase) { ["TABLEPREFIX"] = "Custom_" });

        act.Should().NotThrow();
    }

    // ── MySqlConnectionOptionsBinder / SqlServerConnectionOptionsBinder ─────

    [Fact]
    public void MySqlBinder_SetOptions_Cluster_TablePrefix_SetsPrefix()
    {
        var cluster = Cluster();

        new MySqlConnectionOptionsBinder().SetOptions(cluster, new Dictionary<string, object> { ["tablePrefix"] = "Custom_" });

        cluster.clusterDefinition.AdditionalConnConfig!
            .TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey)
            .Should().Be("Custom_");
    }

    [Fact]
    public void MySqlBinder_SetOptions_UnknownKey_ThrowsArgumentException()
    {
        var cluster = Cluster();

        var act = () => new MySqlConnectionOptionsBinder().SetOptions(cluster, new Dictionary<string, object> { ["unknownProp"] = "x" });

        act.Should().Throw<ArgumentException>().WithMessage("*unknownProp*");
    }

    [Fact]
    public void SqlServerBinder_SetOptions_Cluster_TablePrefix_SetsPrefix()
    {
        var cluster = Cluster();

        new SqlServerConnectionOptionsBinder().SetOptions(cluster, new Dictionary<string, object> { ["tablePrefix"] = "Custom_" });

        cluster.clusterDefinition.AdditionalConnConfig!
            .TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey)
            .Should().Be("Custom_");
    }

    [Fact]
    public void SqlServerBinder_SetOptions_UnknownKey_ThrowsArgumentException()
    {
        var cluster = Cluster();

        var act = () => new SqlServerConnectionOptionsBinder().SetOptions(cluster, new Dictionary<string, object> { ["unknownProp"] = "x" });

        act.Should().Throw<ArgumentException>().WithMessage("*unknownProp*");
    }

    // ── ConfigFromJson wiring ────────────────────────────────────────────────

    [Fact]
    public void ConfigFromJson_PostgresCluster_WithTablePrefix_StoresValueInDefinition()
    {
        const string json = """
        {
            "ClusterId": "C1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "ConnectionOptions": { "tablePrefix": "Custom_" }
        }
        """;

        var cluster = Cluster();
        cluster.ConfigFromJson(json);

        cluster.clusterDefinition.AdditionalConnConfig!
            .TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey)
            .Should().Be("Custom_");
    }

    [Fact]
    public void ConfigFromJson_PostgresAgent_WithTablePrefix_StoresValueInDefinition()
    {
        const string json = """
        {
            "ClusterId": "C1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "AgentConnections": [{
                "Name": "Postgres-1",
                "RepositoryType": "Postgres",
                "ConnectionString": "Host=localhost;",
                "ConnectionOptions": { "tablePrefix": "Custom_" }
            }]
        }
        """;

        var cluster = Cluster();
        cluster.ConfigFromJson(json);

        cluster.clusterDefinition.AgentConnections[0].AgentAdditionalConnConfig!
            .TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey)
            .Should().Be("Custom_");
    }

    [Fact]
    public void ConfigFromJson_PostgresCluster_WithUnknownConnectionOption_Throws()
    {
        const string json = """
        {
            "ClusterId": "C1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "ConnectionOptions": { "badKey": "x" }
        }
        """;

        var cluster = Cluster();
        var act = () => cluster.ConfigFromJson(json);

        act.Should().Throw<ArgumentException>().WithMessage("*badKey*");
    }
}
