using FluentAssertions;
using JobMaster;
using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Abstractions.Models;
using JobMaster.MySql;
using JobMaster.NatsJetStream;
using JobMaster.NatsJetStream.Background;
using JobMaster.Postgres;
using JobMaster.RavenDB;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Repositories;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Services;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background;
using JobMaster.Sdk.Background.Runners;
using JobMaster.Sdk.Background.Runners.DrainRunners;
using JobMaster.Sdk.Background.Runners.JobsExecution;
using JobMaster.Sdk.Background.Runners.SavePendingJobs;
using JobMaster.Sdk.Background.Runners.SavePendingRecurringSchedule;
using JobMaster.Sdk.Ioc;
using JobMaster.Sdk.Ioc.Setup;
using JobMaster.Sdk.Repositories;
using JobMaster.Sdk.Utils;
using JobMaster.SqlBase;
using JobMaster.SqlServer;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using NATS.Client.Core;

namespace JobMaster.UnitTests.Ioc;

public class HybridAgentConnectionTests
{
    static HybridAgentConnectionTests()
    {
        _ = typeof(PostgresConnectionOptionsBinder);
        _ = typeof(MySqlConnectionOptionsBinder);
        _ = typeof(SqlServerConnectionOptionsBinder);
        _ = typeof(RavenDbConnectionOptionsBinder);
        _ = typeof(NatsJetStreamConnectionOptionsBinder);

        BootstrapBlueprintDefinitions.JobMasterScheduler = JobMasterScheduler.Instance;
        BootstrapBlueprintDefinitions.JobMasterRuntime = new JobMasterRuntime();
    }

    private static string UniqueClusterId() => $"Hyb-{Guid.NewGuid():N}"[..16];

    private static ClusterConfigBuilder CreateBuilder(string clusterId, IServiceCollection? services = null)
    {
        BootstrapBlueprintDefinitions.JobMasterScheduler ??= JobMasterScheduler.Instance;
        BootstrapBlueprintDefinitions.JobMasterRuntime ??= new JobMasterRuntime();
        return new ClusterConfigBuilder(clusterId, services ?? new ServiceCollection());
    }

    // ── HybridRepoType helper tests ─────────────────────────────────────────

    [Fact]
    public void HybridRepoType_FormatAndTryParse_RoundTrips()
    {
        var formatted = RepoTypeAndFingerprintUtil.FormatHybridRepoType("NatsJetStream", "RavenDB");

        formatted.Should().Be("hybrid:NatsJetStream&&RavenDB");
        RepoTypeAndFingerprintUtil.IsHybrid(formatted).Should().BeTrue();
        var parsed = RepoTypeAndFingerprintUtil.TryParseHybridRepoType(formatted);
        parsed.Should().NotBeNull();
        parsed!.Value.Transport.Should().Be("NatsJetStream");
        parsed.Value.Execution.Should().Be("RavenDB");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("Postgres")]
    [InlineData("hybrid:")]
    [InlineData("hybrid:Postgres")]
    [InlineData("hybrid:&&Postgres")]
    [InlineData("hybrid:Postgres&&")]
    [InlineData("hybrid:Postgres&&MySql&&RavenDB")]
    [InlineData("hybrid:Bad!&&Postgres")]
    public void HybridRepoType_TryParse_InvalidStrings_ReturnNull(string? value)
    {
        RepoTypeAndFingerprintUtil.TryParseHybridRepoType(value).Should().BeNull();
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("hybrid:Postgres")]
    [InlineData("HYBRID:Postgres")]
    [InlineData("Postgres&&MySql")]
    [InlineData("Postgres:Custom")]
    public void ValidateSingleRepoTypeId_ReservedTokens_ThrowsInvalidOperationException(string invalidRepoType)
    {
        var act = () => RepoTypeAndFingerprintUtil.ValidateSingleRepoTypeId(invalidRepoType);

        act.Should().Throw<InvalidOperationException>();
    }

    [Theory]
    [InlineData("Postgres", true)]
    [InlineData("Nats-JetStream_2", true)]
    [InlineData("Post gres", false)]
    [InlineData("Postgres.v2", false)]
    [InlineData("Postgres\n", false)]
    [InlineData("Postgrés", false)]
    [InlineData("Постгрес", false)]
    public void ValidateSingleRepoTypeId_AllowsOnlyAsciiLettersDigitsDashUnderscore(string repoType, bool expectedValid)
    {
        var act = () => RepoTypeAndFingerprintUtil.ValidateSingleRepoTypeId(repoType);

        if (expectedValid)
            act.Should().NotThrow();
        else
            act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ValidateSingleRepoTypeId_RejectsOverMaxLength()
    {
        // Limit is 50 characters.
        var atLimit = () => RepoTypeAndFingerprintUtil.ValidateSingleRepoTypeId(new string('a', 50));
        atLimit.Should().NotThrow();

        var act = () => RepoTypeAndFingerprintUtil.ValidateSingleRepoTypeId(new string('a', 51));
        act.Should().Throw<InvalidOperationException>();
    }

    private static AgentConnectionDefinition SingleDef(string name, string? repoType, string? connString)
    {
        var def = new AgentConnectionDefinition { AgentConnectionName = name };
        def.Transport.AgentRepoType = repoType;
        def.Transport.AgentConnString = connString;
        return def;
    }

    private static AgentConnectionDefinition HybridDef(
        string name, string? tRepoType, string? tConnString, string? eRepoType, string? eConnString)
    {
        var def = new AgentConnectionDefinition { AgentConnectionName = name };
        def.UseHybrid();
        def.Transport.AgentRepoType = tRepoType;
        def.Transport.AgentConnString = tConnString;
        def.Execution.AgentRepoType = eRepoType;
        def.Execution.AgentConnString = eConnString;
        return def;
    }

    [Fact]
    public void AgentConnectionDefinition_Validate_SingleWithRawHybridRepoType_Throws()
    {
        var def = SingleDef("a1", "hybrid:Postgres&&MySql", "Host=localhost");

        def.Invoking(d => d.Validate()).Should().Throw<ArgumentException>().WithMessage("*Invalid non-hybrid RepositoryType*");
    }

    [Fact]
    public void AgentConnectionDefinition_Validate_HybridWithNestedHybridLeg_Throws()
    {
        var def = HybridDef("a1", "hybrid:Postgres&&MySql", "x", "Postgres", "y");

        def.Invoking(d => d.Validate()).Should().Throw<ArgumentException>().WithMessage("*Invalid non-hybrid RepositoryType*");
        def.AgentRepoType.Should().BeNull(); // not composed while a leg is invalid
    }

    [Theory]
    [InlineData(true, "Transport")]
    [InlineData(false, "Execution")]
    public void AgentConnectionDefinition_Validate_HybridMissingLeg_ThrowsNamingTheLeg(bool missingTransport, string role)
    {
        var def = missingTransport
            ? HybridDef("a1", null, null, "Postgres", "x")
            : HybridDef("a1", "Postgres", "x", null, null);

        def.Invoking(d => d.Validate()).Should().Throw<ArgumentException>()
            .WithMessage($"*'a1'*{role}*");
    }

    [Fact]
    public void AgentConnectionDefinition_SingleAndHybrid_ComputedFields()
    {
        var single = SingleDef("a1", "Postgres", "x");
        single.IsHybrid.Should().BeFalse();
        single.Transport.Should().BeSameAs(single.Execution);
        single.AgentRepoType.Should().Be("Postgres");
        single.AgentConnString.Should().Be("x");
        single.GetProviderRepoTypes().Should().Equal("Postgres");

        var hybrid = HybridDef("a2", "NatsJetStream", "nats://x", "RavenDB", "Urls=x");
        hybrid.IsHybrid.Should().BeTrue();
        hybrid.AgentRepoType.Should().Be("hybrid:NatsJetStream&&RavenDB");
        hybrid.AgentConnString.Should().BeNull();
        hybrid.GetProviderRepoTypes().Should().Equal("NatsJetStream", "RavenDB");
    }

    [Fact]
    public void AgentConnectionDefinition_UseHybrid_AfterSingleConfigured_Throws()
    {
        var def = SingleDef("a1", "Postgres", "x");

        def.Invoking(d => d.UseHybrid()).Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void AgentConnectionDefinition_UseHybrid_AfterAdditionalConfigOnly_Throws()
    {
        var def = new AgentConnectionDefinition { AgentConnectionName = "a1" };
        def.Transport.AgentAdditionalConnConfig = new JobMasterConfigDictionary();

        def.Invoking(d => d.UseHybrid()).Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void AgentConnectionDefinition_Validate_ValidSingleAndHybrid_DoNotThrow()
    {
        SingleDef("a1", "Postgres", "x").Invoking(d => d.Validate()).Should().NotThrow();
        HybridDef("a2", "NatsJetStream", "nats://x", "RavenDB", "Urls=x").Invoking(d => d.Validate()).Should().NotThrow();
    }

    [Fact]
    public void HybridRepoType_FormatFingerprint_ComposesExpectedString()
    {
        var fp = RepoTypeAndFingerprintUtil.FormatHybridFingerprint("nats-fp-1", "ravendb-fp-2");

        fp.Should().Be("hybrid:nats-fp-1&&ravendb-fp-2");
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("hybrid:abc")]
    [InlineData("fp1&&fp2")]
    public void HybridRepoType_ValidateSingleFingerprint_ReservedTokens_ThrowsInvalidOperationException(string invalidFp)
    {
        var act = () => RepoTypeAndFingerprintUtil.ValidateSingleFingerprint(invalidFp);

        act.Should().Throw<InvalidOperationException>();
    }

    // ── Fluent configuration tests ──────────────────────────────────────────

    [Fact]
    public void FluentConfig_Hybrid_NatsTransportAndRavenDbExecution_RegistersExpectedConfig()
    {
        var clusterId = UniqueClusterId();
        var builder = CreateBuilder(clusterId);
        builder.UsePostgresForMaster("Host=localhost;Database=master;Username=u;Password=p");

        builder.AddAgentConnectionConfig("temp")
            .UseHybrid()
            .AgentConnName("hybrid-conn")
            .Transport(t => t.UseNatsJetStream("nats://localhost:4222"))
            .Execution(e => e.UseRavenDb("Urls=http://localhost:8080;Database=agent_exec", collectionPrefix: "Exec_"))
            .ProtectConnectionChanges(false);

        builder.Finish();

        var clusterCfg = JobMasterClusterConnectionConfig.Get(clusterId, includeNotReady: true);
        var agentCfg = clusterCfg.GetAgentConnectionConfig("hybrid-conn");

        agentCfg.IsHybrid.Should().BeTrue();
        agentCfg.RepositoryTypeId.Should().Be("hybrid:NatsJetStream&&RavenDB");
        agentCfg.TransportConfig.Should().NotBeSameAs(agentCfg.ExecutionConfig);
        agentCfg.TransportConfig.RepositoryTypeId.Should().Be("NatsJetStream");
        agentCfg.TransportConfig.ConnectionString.Should().Be("nats://localhost:4222");
        agentCfg.ExecutionConfig.RepositoryTypeId.Should().Be("RavenDB");
        agentCfg.ExecutionConfig.ConnectionString.Should().Be("Urls=http://localhost:8080;Database=agent_exec");
        agentCfg.ExecutionConfig.GetCollectionPrefix().Should().Be("Exec_");

        var providerConfigs = clusterCfg.GetAllAgentProviderConfigs();
        providerConfigs.Should().Contain(agentCfg.TransportConfig);
        providerConfigs.Should().Contain(agentCfg.ExecutionConfig);
    }

    [Fact]
    public void FluentConfig_UseRavenDb_OnSingleAgentConnection_KeepsAgentSelectorChain()
    {
        var clusterId = UniqueClusterId();
        var builder = CreateBuilder(clusterId);
        builder.UseRavenDb("Urls=http://localhost:8080;Database=master");

        // Generic T keeps IAgentConnectionConfigSelector, so ProtectConnectionChanges stays chainable.
        builder.AddAgentConnectionConfig("raven-agent")
            .UseRavenDb("Urls=http://localhost:8080;Database=agent", collectionPrefix: "Agent_")
            .ProtectConnectionChanges(false);

        builder.Finish();

        var agentCfg = JobMasterClusterConnectionConfig.Get(clusterId, includeNotReady: true).GetAgentConnectionConfig("raven-agent");
        agentCfg.IsHybrid.Should().BeFalse();
        agentCfg.RepositoryTypeId.Should().Be("RavenDB");
        agentCfg.TransportConfig.ConnectionString.Should().Be("Urls=http://localhost:8080;Database=agent");
        agentCfg.TransportConfig.GetCollectionPrefix().Should().Be("Agent_");
    }

    [Fact]
    public void FluentConfig_SingleProvider_TransportAndExecutionAreSameInstance()
    {
        var clusterId = UniqueClusterId();
        var builder = CreateBuilder(clusterId);
        builder.UsePostgresForMaster("Host=localhost;Database=master;Username=u;Password=p");
        builder.AddAgentConnectionConfig("single-agent")
            .UsePostgresForAgent("Host=localhost;Database=agent;Username=u;Password=p");

        builder.Finish();

        var clusterCfg = JobMasterClusterConnectionConfig.Get(clusterId, includeNotReady: true);
        var agentCfg = clusterCfg.GetAgentConnectionConfig("single-agent");

        agentCfg.IsHybrid.Should().BeFalse();
        agentCfg.RepositoryTypeId.Should().Be("Postgres");
        agentCfg.TransportConfig.Should().BeSameAs(agentCfg.ExecutionConfig);
        agentCfg.TransportConfig.RepositoryTypeId.Should().Be("Postgres");

        clusterCfg.GetAllAgentProviderConfigs().Where(p => ReferenceEquals(p, agentCfg.TransportConfig))
            .Should().ContainSingle();
    }

    [Fact]
    public void FluentConfig_Hybrid_SameProviderWithSameConnectionString_IsAllowed()
    {
        var clusterId = UniqueClusterId();
        const string connStr = "Host=localhost;Database=agent;Username=u;Password=p";
        var builder = CreateBuilder(clusterId);
        builder.UsePostgresForMaster("Host=localhost;Database=master;Username=u;Password=p");

        builder.AddAgentConnectionConfig("pg-hybrid")
            .UseHybrid()
            .Transport(t => t.UsePostgresForAgent(connStr, tablePrefix: "T_"))
            .Execution(e => e.UsePostgresForAgent(connStr, tablePrefix: "E_"));

        builder.Finish();

        var clusterCfg = JobMasterClusterConnectionConfig.Get(clusterId, includeNotReady: true);
        var agentCfg = clusterCfg.GetAgentConnectionConfig("pg-hybrid");

        agentCfg.IsHybrid.Should().BeTrue();
        agentCfg.RepositoryTypeId.Should().Be("hybrid:Postgres&&Postgres");
        agentCfg.TransportConfig.ConnectionString.Should().Be(connStr);
        agentCfg.ExecutionConfig.ConnectionString.Should().Be(connStr);
        agentCfg.TransportConfig.AdditionalConnConfig
            .TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey)
            .Should().Be("T_");
        agentCfg.ExecutionConfig.AdditionalConnConfig
            .TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey)
            .Should().Be("E_");
    }

    [Fact]
    public void FluentConfig_MixSingleProviderThenHybrid_ThrowsInvalidOperationException()
    {
        var builder = CreateBuilder(UniqueClusterId());
        var agentSel = builder.AddAgentConnectionConfig("conn1")
            .UsePostgresForAgent("Host=localhost;Database=agent;");

        var act = () => agentSel.UseHybrid();

        act.Should().Throw<InvalidOperationException>().WithMessage("*single provider*UseHybrid()*");
    }

    [Fact]
    public void FluentConfig_MixHybridThenSingleProvider_ThrowsInvalidOperationException()
    {
        var builder = CreateBuilder(UniqueClusterId());
        var agentSel = builder.AddAgentConnectionConfig("conn1");
        _ = agentSel.UseHybrid();

        var act = () => agentSel.UsePostgresForAgent("Host=localhost;Database=agent;");

        act.Should().Throw<InvalidOperationException>().WithMessage("*single provider*UseHybrid()*");
    }

    [Fact]
    public void FluentConfig_HybridMissingExecutionLeg_FinishThrowsArgumentException()
    {
        var builder = CreateBuilder(UniqueClusterId());
        builder.UsePostgresForMaster("Host=localhost;Database=master;Username=u;Password=p");
        builder.AddAgentConnectionConfig("incomplete")
            .UseHybrid()
            .Transport(t => t.UseNatsJetStream("nats://localhost:4222"));

        var act = () => builder.Finish();

        act.Should().Throw<ArgumentException>().WithMessage("*Execution*");
    }

    // ── JSON configuration tests ────────────────────────────────────────────

    [Fact]
    public void ConfigFromJson_HybridAgent_WithConnectionOptions_BindsBothLegs()
    {
        const string json = """
        {
            "ClusterId": "C1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "AgentConnections": [{
                "Name": "Hybrid-1",
                "ProtectConnectionChanges": false,
                "Transport": {
                    "RepositoryType": "NatsJetStream",
                    "ConnectionString": "nats://localhost:4222",
                    "ConnectionOptions": {
                        "username": "natsuser",
                        "password": "natssecret"
                    }
                },
                "Execution": {
                    "RepositoryType": "RavenDB",
                    "ConnectionString": "Urls=http://localhost:8080;Database=ExecDb",
                    "ConnectionOptions": {
                        "collectionPrefix": "Exec_"
                    }
                }
            }]
        }
        """;

        var builder = new ClusterConfigBuilder(null, new ServiceCollection());
        builder.ConfigFromJson(json);

        var def = builder.clusterDefinition.AgentConnections.Single();
        def.AgentConnectionName.Should().Be("Hybrid-1");
        def.IsHybrid.Should().BeTrue();
        def.ProtectConnectionChanges.Should().BeFalse();

        def.Transport.Should().NotBeNull();
        def.Transport!.AgentRepoType.Should().Be("NatsJetStream");
        def.Transport.AgentConnString.Should().Be("nats://localhost:4222");
        def.Transport.AgentAdditionalConnConfig!
            .TryGetValue<NatsAuthOpts>(NatsJetStreamConfigKey.NamespaceUniqueKey, NatsJetStreamConfigKey.NatsAuthOptsKey)
            .Should().BeEquivalentTo(new { Username = "natsuser", Password = "natssecret" });

        def.Execution.Should().NotBeNull();
        def.Execution!.AgentRepoType.Should().Be("RavenDB");
        def.Execution.AgentConnString.Should().Be("Urls=http://localhost:8080;Database=ExecDb");
        def.Execution.AgentAdditionalConnConfig!
            .TryGetValue<string>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CollectionPrefixKey)
            .Should().Be("Exec_");
    }

    [Theory]
    [InlineData("""
        "RepositoryType": "Postgres",
        "Transport": { "RepositoryType": "NatsJetStream", "ConnectionString": "nats://localhost:4222" },
        "Execution": { "RepositoryType": "RavenDB", "ConnectionString": "Urls=http://localhost:8080;Database=ExecDb" }
        """)]
    [InlineData("""
        "ConnectionString": "Host=localhost;Database=agent;",
        "Transport": { "RepositoryType": "NatsJetStream", "ConnectionString": "nats://localhost:4222" },
        "Execution": { "RepositoryType": "RavenDB", "ConnectionString": "Urls=http://localhost:8080;Database=ExecDb" }
        """)]
    [InlineData("""
        "ConnectionOptions": { "tablePrefix": "jm_" },
        "Transport": { "RepositoryType": "NatsJetStream", "ConnectionString": "nats://localhost:4222" },
        "Execution": { "RepositoryType": "RavenDB", "ConnectionString": "Urls=http://localhost:8080;Database=ExecDb" }
        """)]
    [InlineData("""
        "ConnectionString": "Host=localhost;Database=agent;",
        "Transport": { "RepositoryType": "NatsJetStream", "ConnectionString": "nats://localhost:4222" }
        """)]
    [InlineData("""
        "RepositoryType": "Postgres",
        "Execution": { "RepositoryType": "RavenDB", "ConnectionString": "Urls=http://localhost:8080;Database=ExecDb" }
        """)]
    public void ConfigFromJson_HybridAgent_MixingSingleAndHybridFields_ThrowsArgumentException(string agentBodyJson)
    {
        var json = $$"""
        {
            "ClusterId": "C1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "AgentConnections": [{
                "Name": "Hybrid-Mix",
                {{agentBodyJson}}
            }]
        }
        """;

        var builder = new ClusterConfigBuilder(null, new ServiceCollection());
        var act = () => builder.ConfigFromJson(json);

        act.Should().Throw<ArgumentException>().WithMessage("*cannot mix*");
    }

    [Theory]
    [InlineData("""
        "Transport": {
            "RepositoryType": "NatsJetStream",
            "ConnectionString": "nats://localhost:4222"
        }
        """)]
    [InlineData("""
        "Execution": {
            "RepositoryType": "RavenDB",
            "ConnectionString": "Urls=http://localhost:8080;Database=ExecDb"
        }
        """)]
    public void ConfigFromJson_HybridAgent_MissingOneLegSection_ThrowsArgumentException(string singleLegJson)
    {
        var json = $$"""
        {
            "ClusterId": "C1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "AgentConnections": [{
                "Name": "Hybrid-MissingLeg",
                {{singleLegJson}}
            }]
        }
        """;

        var builder = new ClusterConfigBuilder(null, new ServiceCollection());
        var act = () => builder.ConfigFromJson(json);

        act.Should().Throw<ArgumentException>().WithMessage("*both 'Transport' and 'Execution'*");
    }

    [Theory]
    [InlineData(
        """{ "ConnectionString": "nats://localhost:4222" }""",
        """{ "RepositoryType": "RavenDB", "ConnectionString": "Urls=http://localhost:8080;Database=ExecDb" }""",
        "*Transport*repoType*")]
    [InlineData(
        """{ "RepositoryType": "NatsJetStream" }""",
        """{ "RepositoryType": "RavenDB", "ConnectionString": "Urls=http://localhost:8080;Database=ExecDb" }""",
        "*Transport*connectionString*")]
    [InlineData(
        """{ "RepositoryType": "NatsJetStream", "ConnectionString": "nats://localhost:4222" }""",
        """{ "ConnectionString": "Urls=http://localhost:8080;Database=ExecDb" }""",
        "*Execution*repoType*")]
    [InlineData(
        """{ "RepositoryType": "NatsJetStream", "ConnectionString": "nats://localhost:4222" }""",
        """{ "RepositoryType": "RavenDB" }""",
        "*Execution*connectionString*")]
    public void ConfigFromJson_HybridAgent_IncompleteLegFields_FinishThrowsArgumentException(
        string transportJson,
        string executionJson,
        string expectedMessagePattern)
    {
        var clusterId = UniqueClusterId();
        var json = $$"""
        {
            "ClusterId": "{{clusterId}}",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;Database=master;Username=u;Password=p",
            "AgentConnections": [{
                "Name": "Hybrid-IncompleteLeg",
                "Transport": {{transportJson}},
                "Execution": {{executionJson}}
            }]
        }
        """;

        var builder = CreateBuilder(clusterId);
        builder.ConfigFromJson(json);

        var act = () => builder.Finish();

        act.Should().Throw<ArgumentException>().WithMessage(expectedMessagePattern);
    }

    // ── NATS TransientThreshold validation tests ────────────────────────────

    [Fact]
    public async Task NatsRuntimeValidate_NatsOnTransportOnly_AllowsTransientThresholdAbove5Minutes()
    {
        var clusterId = UniqueClusterId();
        var services = new ServiceCollection();
        var builder = CreateBuilder(clusterId, services);
        builder
            .UsePostgresForMaster("Host=localhost;Database=master;Username=u;Password=p")
            .TransientThreshold(TimeSpan.FromMinutes(30));

        builder.AddAgentConnectionConfig("nats-transport-only")
            .UseHybrid()
            .Transport(t => t.UseNatsJetStream("nats://localhost:4222"))
            .Execution(e => e.UseRavenDb("Urls=http://localhost:8080;Database=exec"));

        builder.Finish();

        var setup = new NatsJetStreamJobMasterRuntimeSetup();
        var errors = await setup.ValidateAsync(services.BuildServiceProvider());

        errors.Where(e => e.Contains(clusterId)).Should().BeEmpty();
    }

    [Fact]
    public async Task NatsRuntimeValidate_NatsOnExecutionInHybrid_RejectsTransientThresholdAbove5Minutes()
    {
        var clusterId = UniqueClusterId();
        var services = new ServiceCollection();
        var builder = CreateBuilder(clusterId, services);
        builder
            .UsePostgresForMaster("Host=localhost;Database=master;Username=u;Password=p")
            .TransientThreshold(TimeSpan.FromMinutes(30));

        builder.AddAgentConnectionConfig("nats-execution")
            .UseHybrid()
            .Transport(t => t.UsePostgresForAgent("Host=localhost;Database=trans;Username=u;Password=p"))
            .Execution(e => e.UseNatsJetStream("nats://localhost:4222"));

        builder.Finish();

        var setup = new NatsJetStreamJobMasterRuntimeSetup();
        var errors = await setup.ValidateAsync(services.BuildServiceProvider());

        errors.Where(e => e.Contains(clusterId)).Should().ContainSingle()
            .Which.Should().Contain("TransientThreshold");
    }

    [Fact]
    public async Task NatsRuntimeValidate_StandaloneNatsAgent_RejectsTransientThresholdAbove5Minutes()
    {
        var clusterId = UniqueClusterId();
        var services = new ServiceCollection();
        var builder = CreateBuilder(clusterId, services);
        builder
            .UsePostgresForMaster("Host=localhost;Database=master;Username=u;Password=p")
            .TransientThreshold(TimeSpan.FromMinutes(30));

        builder.AddAgentConnectionConfig("nats-single")
            .UseNatsJetStream("nats://localhost:4222");

        builder.Finish();

        var setup = new NatsJetStreamJobMasterRuntimeSetup();
        var errors = await setup.ValidateAsync(services.BuildServiceProvider());

        errors.Where(e => e.Contains(clusterId)).Should().ContainSingle()
            .Which.Should().Contain("TransientThreshold");
    }

    // ── Dispatcher, Fingerprint, Throttler, and Runner resolution tests ─────

    [Fact]
    public void ComponentFactory_SinglePostgres_ResolvesSingleDispatcher()
    {
        var clusterId = UniqueClusterId();
        var builder = CreateBuilder(clusterId);
        builder.UsePostgresForMaster("Host=localhost;Database=master;Username=u;Password=p");
        builder.AddAgentConnectionConfig("single-agent")
            .UsePostgresForAgent("Host=localhost;Database=agent;Username=u;Password=p");

        builder.Finish();

        JobMasterClusterConnectionConfig.Get(clusterId, includeNotReady: true).MarkAsReady();

        var agentComponentFactory = JobMasterClusterAwareComponentFactories.GetFactory(clusterId)
            .GetComponent<IAgentComponentFactory>();

        var dispatcher = agentComponentFactory.GetRepository(new AgentConnectionId(clusterId, "single-agent"));
        dispatcher.Should().BeOfType<SingleAgentJobsDispatcherRepository>();
        dispatcher.AgentRepoTypeId.Should().Be("Postgres");
        dispatcher.IsPollingBasedForSaving.Should().BeTrue();
        dispatcher.IsPollingBasedForProcessing.Should().BeTrue();

        // Already initialized by the factory; a second call must fail, not be silently ignored.
        var agentConfig = JobMasterClusterConnectionConfig.Get(clusterId, includeNotReady: true)
            .GetAgentConnectionConfig("single-agent");
        var act = () => dispatcher.Initialize(agentConfig);
        act.Should().Throw<InvalidOperationException>().WithMessage("*already initialized*");
    }

    [Fact]
    public void RawMessagesRepository_InitializeTwice_Throws()
    {
        var clusterId = UniqueClusterId();
        var builder = CreateBuilder(clusterId);
        builder.UsePostgresForMaster("Host=localhost;Database=master;Username=u;Password=p");
        builder.AddAgentConnectionConfig("single-agent")
            .UsePostgresForAgent("Host=localhost;Database=agent;Username=u;Password=p");

        builder.Finish();

        var agentConfig = JobMasterClusterConnectionConfig.Get(clusterId, includeNotReady: true)
            .GetAgentConnectionConfig("single-agent");
        var rawRepo = JobMasterClusterAwareComponentFactories.GetFactory(clusterId).ClusterServiceProvider
            .GetRequiredKeyedService<IAgentRawMessagesDispatcherRepository>(
                ClusterServiceKeys.GetAgentRawJobsDispatcherSavingKey("Postgres"));

        rawRepo.IsInitialized.Should().BeFalse();
        rawRepo.Initialize(agentConfig.Id, agentConfig.TransportConfig);
        rawRepo.IsInitialized.Should().BeTrue();

        var act = () => rawRepo.Initialize(agentConfig.Id, agentConfig.TransportConfig);
        act.Should().Throw<InvalidOperationException>().WithMessage("*already initialized*");
    }

    [Fact]
    public void ComponentFactory_SinglePostgres_ResolvesSingleFingerprintResolver()
    {
        var clusterId = UniqueClusterId();
        var builder = CreateBuilder(clusterId);
        builder.UsePostgresForMaster("Host=localhost;Database=master;Username=u;Password=p");
        builder.AddAgentConnectionConfig("single-agent")
            .UsePostgresForAgent("Host=localhost;Database=agent;Username=u;Password=p");

        builder.Finish();

        JobMasterClusterConnectionConfig.Get(clusterId, includeNotReady: true).MarkAsReady();

        var agentComponentFactory = JobMasterClusterAwareComponentFactories.GetFactory(clusterId)
            .GetComponent<IAgentComponentFactory>();

        var resolver = agentComponentFactory.GetFingerprintResolver(new AgentConnectionId(clusterId, "single-agent"));
        resolver.Should().BeOfType<SingleAgentFingerprintResolver>();
        resolver.AgentRepoTypeId.Should().Be("Postgres");

        // Already initialized by the factory; a second call must fail, not be silently ignored.
        var agentConfig = JobMasterClusterConnectionConfig.Get(clusterId, includeNotReady: true)
            .GetAgentConnectionConfig("single-agent");
        var act = () => resolver.Initialize(agentConfig);
        act.Should().Throw<InvalidOperationException>().WithMessage("*already initialized*");
    }

    [Fact]
    public void RawFingerprintResolver_InitializeTwice_Throws()
    {
        var clusterId = UniqueClusterId();
        var builder = CreateBuilder(clusterId);
        builder.UsePostgresForMaster("Host=localhost;Database=master;Username=u;Password=p");
        builder.AddAgentConnectionConfig("single-agent")
            .UsePostgresForAgent("Host=localhost;Database=agent;Username=u;Password=p");

        builder.Finish();

        var agentConfig = JobMasterClusterConnectionConfig.Get(clusterId, includeNotReady: true)
            .GetAgentConnectionConfig("single-agent");
        var rawResolver = JobMasterClusterAwareComponentFactories.GetFactory(clusterId).ClusterServiceProvider
            .GetAgentRawFingerprintResolver("Postgres");

        rawResolver.IsInitialized.Should().BeFalse();
        rawResolver.Initialize(agentConfig.Id, agentConfig.TransportConfig);
        rawResolver.IsInitialized.Should().BeTrue();

        var act = () => rawResolver.Initialize(agentConfig.Id, agentConfig.TransportConfig);
        act.Should().Throw<InvalidOperationException>().WithMessage("*already initialized*");
    }

    [Fact]
    public async Task SingleAgentFingerprintResolver_ReturnsRawFingerprintUnchanged_AndInitializesSharedResolverOnce()
    {
        var raw = new Mock<IAgentRawFingerprintResolver>();
        var initialized = false;
        raw.SetupGet(r => r.IsInitialized).Returns(() => initialized);
        raw.Setup(r => r.Initialize(It.IsAny<string>(), It.IsAny<JobMasterAgentProviderConfig>()))
            .Callback(() => initialized = true);
        raw.Setup(r => r.GiveYourFingerprintAsync("C1", "C1:single")).ReturnsAsync("fp-raw");

        var resolver = new SingleAgentFingerprintResolver("Postgres", raw.Object);
        var cfg = new JobMasterAgentConnectionConfig("C1", "single", "Host=localhost", "Postgres");

        resolver.Initialize(cfg);

        raw.Verify(r => r.Initialize("C1:single", cfg.TransportConfig), Times.Once);
        (await resolver.GiveYourFingerprintAsync("C1", "C1:single")).Should().Be("fp-raw");
    }

    [Fact]
    public void ComponentFactory_HybridNatsAndRavenDb_ResolvesHybridDispatcherAndRoleRunners()
    {
        var clusterId = UniqueClusterId();
        var builder = CreateBuilder(clusterId);
        builder.UsePostgresForMaster("Host=localhost;Database=master;Username=u;Password=p");
        builder.AddAgentConnectionConfig("hyb-agent")
            .UseHybrid()
            .Transport(t => t.UseNatsJetStream("nats://localhost:4222"))
            .Execution(e => e.UseRavenDb("Urls=http://localhost:8080;Database=exec"));

        builder.Finish();

        var clusterCfg = JobMasterClusterConnectionConfig.Get(clusterId, includeNotReady: true);
        clusterCfg.MarkAsReady();

        var componentFactory = JobMasterClusterAwareComponentFactories.GetFactory(clusterId);
        var agentComponentFactory = componentFactory.GetComponent<IAgentComponentFactory>();
        var agentConnId = new AgentConnectionId(clusterId, "hyb-agent");

        var dispatcher = agentComponentFactory.GetRepository(agentConnId);
        dispatcher.Should().BeOfType<HybridJobsDispatcherRepository>();
        dispatcher.AgentRepoTypeId.Should().Be("hybrid:NatsJetStream&&RavenDB");
        dispatcher.IsPollingBasedForSaving.Should().BeFalse();     // NatsJetStream transport is push/stream-based
        dispatcher.IsPollingBasedForProcessing.Should().BeTrue();  // RavenDB execution is polling-based

        var fingerprintResolver = agentComponentFactory.GetFingerprintResolver(agentConnId);
        fingerprintResolver.Should().BeOfType<HybridAgentFingerprintResolver>();
        fingerprintResolver.AgentRepoTypeId.Should().Be("hybrid:NatsJetStream&&RavenDB");

        // Role-specific throttler batch sizes resolve without throwing on hybrid RepoType
        OperationThrottlerSettingsFactory.GetAgentTransportMaxBatchSize(agentConnId.IdValue)
            .Should().BeGreaterThan(0);
        OperationThrottlerSettingsFactory.GetAgentExecutionMaxBatchSize(agentConnId.IdValue)
            .Should().BeGreaterThan(0);

        // Verify BucketRunnersFactory selects NATS runners for Transport and Polling runners for Execution
        var mockWorker = new Mock<IJobMasterBackgroundAgentWorker>();
        mockWorker.Setup(w => w.ClusterConnConfig).Returns(clusterCfg);
        mockWorker.Setup(w => w.GetClusterAwareService<IMasterJobsService>())
            .Returns(componentFactory.GetService<IMasterJobsService>());
        mockWorker.Setup(w => w.GetClusterAwareService<IMasterBucketsService>())
            .Returns(componentFactory.GetService<IMasterBucketsService>());
        mockWorker.Setup(w => w.GetClusterAwareService<IMasterClusterConfigurationService>())
            .Returns(componentFactory.GetService<IMasterClusterConfigurationService>());
        mockWorker.Setup(w => w.GetClusterAwareService<IMasterRecurringSchedulesService>())
            .Returns(componentFactory.GetService<IMasterRecurringSchedulesService>());
        mockWorker.Setup(w => w.GetClusterAwareService<IRecurringSchedulePlanner>())
            .Returns(componentFactory.GetService<IRecurringSchedulePlanner>());
        mockWorker.Setup(w => w.GetClusterAwareService<IMasterDistributedLockerService>())
            .Returns(componentFactory.GetService<IMasterDistributedLockerService>());
        mockWorker.Setup(w => w.GetClusterAwareService<IJobMasterLogger>())
            .Returns(componentFactory.GetComponent<IJobMasterLogger>());

        var runnersFactory = componentFactory.GetComponent<IBucketRunnersFactory>();

        runnersFactory.NewSavePendingJobsRunner(mockWorker.Object, agentConnId)
            .Should().BeOfType<NatsJetStreamSavePendingJobsRunner>();
        runnersFactory.NewSaveRecurringSchedulerRunner(mockWorker.Object, agentConnId)
            .Should().BeOfType<NatsJetStreamSaveRecurringScheduleRunner>();
        runnersFactory.NewDrainSavePendingJobsRunner(mockWorker.Object, agentConnId)
            .Should().BeOfType<NatsJetStreamDrainSavePendingJobsRunner>();
        runnersFactory.NewDrainSavePendingRecurringScheduleRunner(mockWorker.Object, agentConnId)
            .Should().BeOfType<NatsJetStreamDrainSavePendingRecurringScheduleRunner>();
        runnersFactory.NewDrainProcessingJobsRunner(mockWorker.Object, agentConnId)
            .Should().BeOfType<PollingDrainProcessingJobsRunner>();
    }

    [Fact]
    public async Task HybridAgentFingerprintResolver_ComposesTransportAndExecutionFingerprints()
    {
        var mockTransport = new Mock<IAgentRawFingerprintResolver>();
        mockTransport.Setup(r => r.GiveYourFingerprintAsync("C1", "C1:hyb"))
            .ReturnsAsync("fp-transport");

        var mockExecution = new Mock<IAgentRawFingerprintResolver>();
        mockExecution.Setup(r => r.GiveYourFingerprintAsync("C1", "C1:hyb"))
            .ReturnsAsync("fp-execution");

        var resolver = new HybridAgentFingerprintResolver(
            "hybrid:NatsJetStream&&RavenDB",
            mockTransport.Object,
            mockExecution.Object);

        var tCfg = new JobMasterAgentProviderConfig("NatsJetStream", "nats://localhost:4222");
        var eCfg = new JobMasterAgentProviderConfig("RavenDB", "Urls=http://localhost:8080;Database=db");
        var hCfg = new JobMasterAgentConnectionConfig("C1", "hyb", tCfg, eCfg);

        resolver.Initialize(hCfg);

        mockTransport.Verify(r => r.Initialize("C1:hyb", tCfg), Times.Once);
        mockExecution.Verify(r => r.Initialize("C1:hyb", eCfg), Times.Once);

        var combined = await resolver.GiveYourFingerprintAsync("C1", "C1:hyb");
        combined.Should().Be("hybrid:fp-transport&&fp-execution");
    }

    [Theory]
    [InlineData("Postgres", "abc123", true)]
    [InlineData("hybrid:NatsJetStream&&RavenDB", "hybrid:fp1&&fp2", true)]
    [InlineData("Postgres", "hybrid:fp1&&fp2", false)]
    [InlineData("hybrid:NatsJetStream&&RavenDB", "fp1", false)]
    [InlineData("Postgres", "bad&&fingerprint", false)]
    public void RepoTypeAndFingerprintUtil_IsValid_MatchesHybridParityAndTokens(string repoType, string fingerprint, bool expected)
    {
        RepoTypeAndFingerprintUtil.IsValid(repoType, fingerprint).Should().Be(expected);
    }
}
