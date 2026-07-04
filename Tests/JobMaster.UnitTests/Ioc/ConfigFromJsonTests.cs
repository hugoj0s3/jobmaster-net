using System.Text;
using FluentAssertions;
using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions;
using JobMaster.Sdk.Ioc.Setup;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.UnitTests.Ioc;

public class ConfigFromJsonTests
{
    // ── helpers ──────────────────────────────────────────────────────────────

    private static ClusterConfigBuilder Builder() =>
        new(null, new ServiceCollection());

    // ── cluster-level fields ─────────────────────────────────────────────────

    [Fact]
    public void ConfigFromJson_String_MapsClusterFields()
    {
        const string json = """
        {
            "ClusterId": "Cluster-1",
            "SetAsDefault": true,
            "Mode": "Passive",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "TransientThreshold": "00:01:00",
            "DefaultJobTimeout": "00:05:00",
            "DefaultMaxRetryCount": 5,
            "MaxMessageByteSize": 65536,
            "IanaTimeZoneId": "America/New_York"
        }
        """;

        var b = Builder();
        b.ConfigFromJson(json);
        var d = b.clusterDefinition;

        d.ClusterId.Should().Be("Cluster-1");
        d.IsDefault.Should().BeTrue();
        d.ClusterMode.Should().Be(ClusterMode.Passive);
        d.RepoType.Should().Be("Postgres");
        d.ConnString.Should().Be("Host=localhost;");
        d.TransientThreshold.Should().Be(TimeSpan.FromMinutes(1));
        d.DefaultJobTimeout.Should().Be(TimeSpan.FromMinutes(5));
        d.DefaultMaxRetryCount.Should().Be(5);
        d.MaxMessageByteSize.Should().Be(65536);
        d.IanaTimeZoneId.Should().Be("America/New_York");
        d.IsStandalone.Should().BeFalse();
    }

    [Fact]
    public void ConfigFromJson_String_MapsDataRetentionTtl()
    {
        const string json = """
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "DataRetentionTtl": "30.00:00:00"
        }
        """;

        var b = Builder();
        b.ConfigFromJson(json);

        b.clusterDefinition.DataRetentionTtl.Should().Be(TimeSpan.FromDays(30));
    }

    [Fact]
    public void ConfigFromJson_String_WhenDataRetentionTtlAbsent_LeavesItUnset()
    {
        const string json = """
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;"
        }
        """;

        var b = Builder();
        b.ConfigFromJson(json);

        b.clusterDefinition.DataRetentionTtl.Should().BeNull();
    }

    [Theory]
    [InlineData("00:00:00")]
    [InlineData("-1.00:00:00")]
    public void ConfigFromJson_String_WhenDataRetentionTtlZeroOrNegative_SetsTimeSpanZero(string ttlValue)
    {
        var json = $$"""
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "DataRetentionTtl": "{{ttlValue}}"
        }
        """;

        var b = Builder();
        b.ConfigFromJson(json);

        b.clusterDefinition.DataRetentionTtl.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void ConfigFromJson_String_WhenDataRetentionTtlBelowMinimum_Throws()
    {
        const string json = """
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "DataRetentionTtl": "00:01:00"
        }
        """;

        var b = Builder();
        b.Invoking(x => x.ConfigFromJson(json))
            .Should().Throw<ArgumentException>();
    }

    [Fact]
    public void RetainDataForever_SetsTimeSpanZero()
    {
        var b = Builder();
        b.RetainDataForever();

        b.clusterDefinition.DataRetentionTtl.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void DataRetentionTtl_WhenPositiveAndBelowMinimum_Throws()
    {
        var b = Builder();
        b.Invoking(x => x.DataRetentionTtl(TimeSpan.FromMinutes(1)))
            .Should().Throw<ArgumentException>();
    }

    [Fact]
    public void DataRetentionTtl_WhenNegative_SetsTimeSpanZero()
    {
        var b = Builder();
        b.DataRetentionTtl(TimeSpan.FromHours(-1));

        b.clusterDefinition.DataRetentionTtl.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void ConfigFromJson_String_WhenSetAsDefaultFalse_DoesNotSetDefault()
    {
        const string json = """
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "SetAsDefault": false
        }
        """;

        var b = Builder();
        b.ConfigFromJson(json);

        b.clusterDefinition.IsDefault.Should().BeFalse();
    }

    // ── agent connections ────────────────────────────────────────────────────

    [Fact]
    public void ConfigFromJson_String_MapsAgentConnections()
    {
        const string json = """
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "AgentConnections": [
                { "Name": "Nats-1",    "RepositoryType": "NatsJetStream", "ConnectionString": "nats://localhost" },
                { "Name": "Pg-Agent-1","RepositoryType": "Postgres",      "ConnectionString": "Host=agent;" }
            ]
        }
        """;

        var b = Builder();
        b.ConfigFromJson(json);
        var agents = b.clusterDefinition.AgentConnections;

        agents.Should().HaveCount(2);

        agents[0].AgentConnectionName.Should().Be("Nats-1");
        agents[0].AgentRepoType.Should().Be("NatsJetStream");
        agents[0].AgentConnString.Should().Be("nats://localhost");

        agents[1].AgentConnectionName.Should().Be("Pg-Agent-1");
        agents[1].AgentRepoType.Should().Be("Postgres");
        agents[1].AgentConnString.Should().Be("Host=agent;");
    }

    [Fact]
    public void ConfigFromJson_String_AgentConnection_ProtectConnectionChanges_Override()
    {
        const string json = """
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "AgentConnections": [
                { "Name": "Nats-1", "RepositoryType": "NatsJetStream", "ConnectionString": "nats://localhost", "ProtectConnectionChanges": false }
            ]
        }
        """;

        var b = Builder();
        b.ConfigFromJson(json);

        b.clusterDefinition.AgentConnections[0].ProtectConnectionChanges.Should().BeFalse();
    }

    // ── worker fields ────────────────────────────────────────────────────────

    [Fact]
    public void ConfigFromJson_String_MapsWorkerFields()
    {
        const string json = """
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "Workers": [
                {
                    "WorkerName": "Worker-1",
                    "AgentConnectionName": "Nats-1",
                    "WorkerLane": "Lane-A",
                    "TransferBatchSize": 500,
                    "BucketBufferSize": 100,
                    "WorkerMode": "Execution",
                    "ParallelismFactor": 2.0,
                    "SkipWarmUpTime": true,
                    "BucketQtyConfig": {
                        "Medium": 2,
                        "High": 3
                    }
                }
            ]
        }
        """;

        var b = Builder();
        b.ConfigFromJson(json);
        var w = b.clusterDefinition.Workers[0];

        w.WorkerName.Should().Be("Worker-1");
        w.AgentConnectionName.Should().Be("Nats-1");
        w.WorkerLane.Should().Be("Lane-A");
        w.TransferBatchSize.Should().Be(500);
        w.BucketBufferSize.Should().Be(100);
        w.Mode.Should().Be(AgentWorkerMode.Execution);
        w.ParallelismFactor.Should().Be(2.0);
        w.SkipWarmUpTime.Should().BeTrue();
        w.BucketQty[JobMasterPriority.Medium].Should().Be(2);
        w.BucketQty[JobMasterPriority.High].Should().Be(3);
    }

    [Fact]
    public void ConfigFromJson_String_TransferBatchSize_MapsCorrectly()
    {
        const string json = """
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "Workers": [{ "TransferBatchSize": 300 }]
        }
        """;

        var b = Builder();
        b.ConfigFromJson(json);

        b.clusterDefinition.Workers[0].TransferBatchSize.Should().Be(300);
    }

    [Fact]
    public void ConfigFromJson_String_WhenTransferBatchSizeAbsent_UsesDefault()
    {
        const string json = """
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "Workers": [{}]
        }
        """;

        var b = Builder();
        b.ConfigFromJson(json);

        b.clusterDefinition.Workers[0].TransferBatchSize.Should().Be(JobMasterDefaults.Worker.TransferBatchSize);
    }

    [Fact]
    public void ConfigFromJson_String_MultipleWorkersAndAgents_MapsAll()
    {
        const string json = """
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "AgentConnections": [
                { "Name": "Nats-1", "RepositoryType": "NatsJetStream", "ConnectionString": "nats://localhost" },
                { "Name": "Pg-1",   "RepositoryType": "Postgres",      "ConnectionString": "Host=agent;" }
            ],
            "Workers": [
                { "AgentConnectionName": "Nats-1", "WorkerMode": "Full" },
                { "AgentConnectionName": "Nats-1", "WorkerMode": "Drain" }
            ]
        }
        """;

        var b = Builder();
        b.ConfigFromJson(json);

        b.clusterDefinition.AgentConnections.Should().HaveCount(2);
        b.clusterDefinition.Workers.Should().HaveCount(2);
        b.clusterDefinition.Workers[0].Mode.Should().Be(AgentWorkerMode.Full);
        b.clusterDefinition.Workers[1].Mode.Should().Be(AgentWorkerMode.Drain);
    }

    // ── standalone mode ──────────────────────────────────────────────────────

    [Fact]
    public void ConfigFromJson_String_StandaloneCluster_MapsFields()
    {
        const string json = """
        {
            "Standalone": true,
            "ClusterId": "Cluster-Standalone-1",
            "SetAsDefault": true,
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "IanaTimeZoneId": "UTC",
            "Workers": [
                { "WorkerName": "sa-worker", "TransferBatchSize": 300 }
            ]
        }
        """;

        var b = Builder();
        b.ConfigFromJson(json);
        var d = b.clusterDefinition;

        d.IsStandalone.Should().BeTrue();
        d.ClusterId.Should().Be("Cluster-Standalone-1");
        d.IsDefault.Should().BeTrue();
        d.RepoType.Should().Be("Postgres");
        d.ConnString.Should().Be("Host=localhost;");
        d.IanaTimeZoneId.Should().Be("UTC");
        d.Workers.Should().HaveCount(1);
        d.Workers[0].WorkerName.Should().Be("sa-worker");
        d.Workers[0].TransferBatchSize.Should().Be(300);
        d.AgentConnections.Should().BeEmpty();
    }

    // ── enum case-insensitivity ──────────────────────────────────────────────

    [Theory]
    [InlineData("active",   ClusterMode.Active)]
    [InlineData("PASSIVE",  ClusterMode.Passive)]
    [InlineData("Archived", ClusterMode.Archived)]
    public void ConfigFromJson_String_ClusterModeCaseInsensitive(string mode, ClusterMode expected)
    {
        var json = $$"""
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "Mode": "{{mode}}"
        }
        """;

        var b = Builder();
        b.ConfigFromJson(json);

        b.clusterDefinition.ClusterMode.Should().Be(expected);
    }

    [Theory]
    [InlineData("full",        AgentWorkerMode.Full)]
    [InlineData("EXECUTION",   AgentWorkerMode.Execution)]
    [InlineData("Drain",       AgentWorkerMode.Drain)]
    [InlineData("coordinator", AgentWorkerMode.Coordinator)]
    public void ConfigFromJson_String_WorkerModeCaseInsensitive(string mode, AgentWorkerMode expected)
    {
        var json = $$"""
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "Workers": [{ "WorkerMode": "{{mode}}" }]
        }
        """;

        var b = Builder();
        b.ConfigFromJson(json);

        b.clusterDefinition.Workers[0].Mode.Should().Be(expected);
    }

    [Theory]
    [InlineData("verylow",  JobMasterPriority.VeryLow)]
    [InlineData("LOW",      JobMasterPriority.Low)]
    [InlineData("Medium",   JobMasterPriority.Medium)]
    [InlineData("HIGH",     JobMasterPriority.High)]
    [InlineData("critical", JobMasterPriority.Critical)]
    public void ConfigFromJson_String_BucketQtyConfigKeysCaseInsensitive(string key, JobMasterPriority expected)
    {
        var json = $$"""
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "Workers": [{ "BucketQtyConfig": { "{{key}}": 4 } }]
        }
        """;

        var b = Builder();
        b.ConfigFromJson(json);

        b.clusterDefinition.Workers[0].BucketQty[expected].Should().Be(4);
    }

    // ── TimeSpan parsing ─────────────────────────────────────────────────────

    [Theory]
    [InlineData("00:01:00",  60)]
    [InlineData("00:10:00",  600)]
    [InlineData("01:00:00",  3600)]
    public void ConfigFromJson_String_TransientThreshold_ParsedAsTimeSpan(string value, int expectedSeconds)
    {
        var json = $$"""
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "TransientThreshold": "{{value}}"
        }
        """;

        var b = Builder();
        b.ConfigFromJson(json);

        b.clusterDefinition.TransientThreshold.Should().Be(TimeSpan.FromSeconds(expectedSeconds));
    }

    // ── IConfiguration overload ──────────────────────────────────────────────

    [Fact]
    public void ConfigFromJson_IConfiguration_MapsClusterAndWorkerFields()
    {
        var values = new Dictionary<string, string?>
        {
            ["ClusterId"]                      = "Cluster-Config",
            ["RepoType"]                       = "Postgres",
            ["ConnectionString"]               = "Host=localhost;",
            ["SetAsDefault"]                   = "true",
            ["DefaultMaxRetryCount"]           = "7",
            ["IanaTimeZoneId"]                 = "Europe/London",
            ["Workers:0:TransferBatchSize"]      = "400",
            ["Workers:0:WorkerMode"]           = "Drain",
            ["Workers:0:AgentConnectionName"]  = "Nats-1",
            ["AgentConnections:0:Name"]              = "Nats-1",
            ["AgentConnections:0:RepositoryType"]    = "NatsJetStream",
            ["AgentConnections:0:ConnectionString"]  = "nats://localhost"
        };

        var config = new ConfigurationBuilder().AddInMemoryCollection(values).Build();
        var b = Builder();
        b.ConfigFromJson(config);
        var d = b.clusterDefinition;

        d.ClusterId.Should().Be("Cluster-Config");
        d.RepoType.Should().Be("Postgres");
        d.ConnString.Should().Be("Host=localhost;");
        d.IsDefault.Should().BeTrue();
        d.DefaultMaxRetryCount.Should().Be(7);
        d.IanaTimeZoneId.Should().Be("Europe/London");
        d.Workers.Should().HaveCount(1);
        d.Workers[0].TransferBatchSize.Should().Be(400);
        d.Workers[0].Mode.Should().Be(AgentWorkerMode.Drain);
        d.AgentConnections.Should().HaveCount(1);
        d.AgentConnections[0].AgentConnectionName.Should().Be("Nats-1");
    }

    // ── Stream overload ──────────────────────────────────────────────────────

    [Fact]
    public void ConfigFromJson_Stream_MapsFields()
    {
        const string json = """
        {
            "ClusterId": "Cluster-Stream",
            "RepoType": "Postgres",
            "ConnectionString": "Host=stream;",
            "Workers": [{ "WorkerMode": "Full" }]
        }
        """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        var b = Builder();
        b.ConfigFromJson(stream);
        var d = b.clusterDefinition;

        d.ClusterId.Should().Be("Cluster-Stream");
        d.RepoType.Should().Be("Postgres");
        d.ConnString.Should().Be("Host=stream;");
        d.Workers[0].Mode.Should().Be(AgentWorkerMode.Full);
    }

    // ── file path overload ───────────────────────────────────────────────────

    [Fact]
    public void ConfigFromJson_FilePath_LoadsAndMapsFields()
    {
        const string json = """
        {
            "ClusterId": "Cluster-File",
            "RepoType": "Postgres",
            "ConnectionString": "Host=file;",
            "DefaultMaxRetryCount": 9
        }
        """;

        var tempFile = Path.ChangeExtension(Path.GetTempFileName(), ".json");
        try
        {
            File.WriteAllText(tempFile, json);
            var b = Builder();
            b.ConfigFromJson(tempFile);

            b.clusterDefinition.ClusterId.Should().Be("Cluster-File");
            b.clusterDefinition.DefaultMaxRetryCount.Should().Be(9);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    // ── chaining ─────────────────────────────────────────────────────────────

    [Fact]
    public void ConfigFromJson_ReturnsSelector_AllowsFurtherChaining()
    {
        const string json = """
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;"
        }
        """;

        var b = Builder();
        b.ConfigFromJson(json).DefaultMaxRetryCount(99);

        b.clusterDefinition.DefaultMaxRetryCount.Should().Be(99);
    }

    // ── fluent selector (non-JSON) ───────────────────────────────────────────

    [Fact]
    public void DataRetentionTtl_SetViaFluentSelector_UpdatesClusterDefinition()
    {
        var b = Builder();
        b.ClusterId("Cluster-1").DataRetentionTtl(TimeSpan.FromDays(7));

        b.clusterDefinition.DataRetentionTtl.Should().Be(TimeSpan.FromDays(7));
    }

    [Fact]
    public void ClusterDataRetentionTtl_SetViaStandaloneSelector_UpdatesClusterDefinition()
    {
        var b = Builder();
        IClusterStandaloneConfigSelector standalone = b.UseStandaloneCluster();
        standalone.ClusterDataRetentionTtl(TimeSpan.FromDays(14));

        b.clusterDefinition.DataRetentionTtl.Should().Be(TimeSpan.FromDays(14));
    }

    // ── invalid enum ─────────────────────────────────────────────────────────

    [Fact]
    public void ConfigFromJson_InvalidClusterMode_Throws()
    {
        const string json = """
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "Mode": "NotAMode"
        }
        """;

        var b = Builder();
        var act = () => b.ConfigFromJson(json);

        act.Should().Throw<ArgumentException>().WithMessage("*NotAMode*");
    }

    [Fact]
    public void ConfigFromJson_InvalidWorkerMode_Throws()
    {
        const string json = """
        {
            "ClusterId": "Cluster-1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "Workers": [{ "WorkerMode": "Turbo" }]
        }
        """;

        var b = Builder();
        var act = () => b.ConfigFromJson(json);

        act.Should().Throw<ArgumentException>().WithMessage("*Turbo*");
    }
}
