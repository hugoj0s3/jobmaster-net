using FluentAssertions;
using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.NatsJetStream;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Ioc.Setup;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.Core;

namespace JobMaster.UnitTests.Ioc;

public class NatsConnectionOptionsTests
{
    static NatsConnectionOptionsTests()
    {
        // Force the NatsJetStream assembly to load before the binder factory
        // initializes its static discovery scan.
        _ = typeof(NatsJetStreamConnectionOptionsBinder);
    }

    // ── fake agent selector ──────────────────────────────────────────────────

    private sealed class FakeAgentSelector : IAgentConnectionConfigSelector
    {
        public readonly List<(JobMasterNamespaceUniqueKey Ns, string Key, object Value)> Captured = [];

        IAgentConnectionConfigSelector IAgentConnectionConfigSelector.AgentConnName(string _) => this;
        IAgentConnectionConfigSelector IAgentConnectionConfigSelector.ProtectConnectionChanges(bool _) => this;
        IAgentConnectionConfigSelector IAgentConnectionConfigSelector.AgentAdditionalConnConfig(JobMasterConfigDictionary _) => this;
        IAgentConnectionConfigSelector IAgentConnectionConfigSelector.AgentRepoType(string _) => this;
        IAgentConnectionConfigSelector IAgentConnectionConfigSelector.AgentConnString(string _) => this;
        IAgentConnectionConfigSelector IAgentConnectionConfigSelector.RuntimeDbOperationLimit(int _) => this;

        IAgentConnectionConfigSelector IAgentConnectionConfigSelector.AppendAdditionalConnConfigValue(
            JobMasterNamespaceUniqueKey namespaceKey, string key, object value)
        {
            Captured.Add((namespaceKey, key, value));
            return this;
        }
    }

    private static NatsJetStreamConnectionOptionsBinder Binder() => new();
    private static FakeAgentSelector Selector() => new();

    private static NatsAuthOpts CapturedAuth(FakeAgentSelector sel)
        => sel.Captured
              .Where(c => c.Key == NatsJetStreamConfigKey.NatsAuthOptsKey)
              .Select(c => (NatsAuthOpts)c.Value)
              .Single();

    private static NatsTlsOpts CapturedTls(FakeAgentSelector sel)
        => sel.Captured
              .Where(c => c.Key == NatsJetStreamConfigKey.NatsTlsOptsKey)
              .Select(c => (NatsTlsOpts)c.Value)
              .Single();

    // ── auth options ─────────────────────────────────────────────────────────

    [Fact]
    public void SetOptions_UsernamePassword_SetsAuthOpts()
    {
        var sel = Selector();
        Binder().SetOptions(sel, new Dictionary<string, object>
        {
            ["username"] = "alice",
            ["password"] = "secret"
        });

        var auth = CapturedAuth(sel);
        auth.Username.Should().Be("alice");
        auth.Password.Should().Be("secret");
    }

    [Fact]
    public void SetOptions_Token_SetsAuthOpts()
    {
        var sel = Selector();
        Binder().SetOptions(sel, new Dictionary<string, object> { ["token"] = "tok123" });

        CapturedAuth(sel).Token.Should().Be("tok123");
    }

    [Fact]
    public void SetOptions_CredentialsFile_SetsCredsFile()
    {
        var sel = Selector();
        Binder().SetOptions(sel, new Dictionary<string, object> { ["credentialsFile"] = "/etc/nats/my.creds" });

        CapturedAuth(sel).CredsFile.Should().Be("/etc/nats/my.creds");
    }

    [Fact]
    public void SetOptions_NKeyAndJwt_SetsAuthOpts()
    {
        var sel = Selector();
        Binder().SetOptions(sel, new Dictionary<string, object>
        {
            ["nkey"] = "SUANQ...",
            ["jwt"]  = "eyJ..."
        });

        var auth = CapturedAuth(sel);
        auth.NKey.Should().Be("SUANQ...");
        auth.Jwt.Should().Be("eyJ...");
    }

    [Fact]
    public void SetOptions_NoAuthKeys_DoesNotWriteAuthOpts()
    {
        var sel = Selector();
        Binder().SetOptions(sel, new Dictionary<string, object> { ["tlsCaFile"] = "/etc/ca.pem" });

        sel.Captured.Should().NotContain(c => c.Key == NatsJetStreamConfigKey.NatsAuthOptsKey);
    }

    // ── TLS options ──────────────────────────────────────────────────────────

    [Fact]
    public void SetOptions_TlsCertBundleFile_SetsTlsOpts()
    {
        var sel = Selector();
        Binder().SetOptions(sel, new Dictionary<string, object>
        {
            ["tlsCertBundleFile"]         = "/etc/nats/bundle.pfx",
            ["tlsCertBundleFilePassword"] = "pfxpass"
        });

        var tls = CapturedTls(sel);
        tls.CertBundleFile.Should().Be("/etc/nats/bundle.pfx");
        tls.CertBundleFilePassword.Should().Be("pfxpass");
    }

    [Fact]
    public void SetOptions_TlsCaFile_SetsTlsOpts()
    {
        var sel = Selector();
        Binder().SetOptions(sel, new Dictionary<string, object> { ["tlsCaFile"] = "/etc/ca.pem" });

        CapturedTls(sel).CaFile.Should().Be("/etc/ca.pem");
    }

    [Fact]
    public void SetOptions_InsecureSkipVerify_True_SetsFlag()
    {
        var sel = Selector();
        Binder().SetOptions(sel, new Dictionary<string, object>
        {
            ["tlsCaFile"]             = "/etc/ca.pem",
            ["tlsInsecureSkipVerify"] = "true"
        });

        CapturedTls(sel).InsecureSkipVerify.Should().BeTrue();
    }

    [Fact]
    public void SetOptions_InsecureSkipVerify_False_ClearFlag()
    {
        var sel = Selector();
        Binder().SetOptions(sel, new Dictionary<string, object>
        {
            ["tlsCaFile"]             = "/etc/ca.pem",
            ["tlsInsecureSkipVerify"] = "false"
        });

        CapturedTls(sel).InsecureSkipVerify.Should().BeFalse();
    }

    [Theory]
    [InlineData("Require",  TlsMode.Require)]
    [InlineData("disable",  TlsMode.Disable)]
    [InlineData("AUTO",     TlsMode.Auto)]
    [InlineData("Prefer",   TlsMode.Prefer)]
    [InlineData("Implicit", TlsMode.Implicit)]
    public void SetOptions_TlsMode_ParsesCaseInsensitive(string input, TlsMode expected)
    {
        var sel = Selector();
        Binder().SetOptions(sel, new Dictionary<string, object> { ["tlsMode"] = input });

        CapturedTls(sel).Mode.Should().Be(expected);
    }

    [Fact]
    public void SetOptions_InvalidTlsMode_ThrowsArgumentException()
    {
        var sel = Selector();
        var act = () => Binder().SetOptions(sel, new Dictionary<string, object> { ["tlsMode"] = "SuperSecure" });

        act.Should().Throw<ArgumentException>().WithMessage("*SuperSecure*");
    }

    [Fact]
    public void SetOptions_NoTlsKeys_DoesNotWriteTlsOpts()
    {
        var sel = Selector();
        Binder().SetOptions(sel, new Dictionary<string, object> { ["username"] = "u" });

        sel.Captured.Should().NotContain(c => c.Key == NatsJetStreamConfigKey.NatsTlsOptsKey);
    }

    // ── key validation ───────────────────────────────────────────────────────

    [Fact]
    public void SetOptions_UnknownKey_ThrowsArgumentException()
    {
        var sel = Selector();
        var act = () => Binder().SetOptions(sel, new Dictionary<string, object> { ["unknownProp"] = "x" });

        act.Should().Throw<ArgumentException>().WithMessage("*unknownProp*");
    }

    [Theory]
    [InlineData("Username",  "alice")]
    [InlineData("PASSWORD",  "secret")]
    [InlineData("Token",     "tok123")]
    [InlineData("TlsCaFile", "/etc/ca.pem")]
    [InlineData("TLSMODE",   "Require")]
    public void SetOptions_KeysCaseInsensitive_DoesNotThrow(string key, string value)
    {
        var sel = Selector();
        var act = () => Binder().SetOptions(
            sel,
            new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase) { [key] = value });

        act.Should().NotThrow();
    }

    // ── cluster selector ─────────────────────────────────────────────────────

    [Fact]
    public void SetOptions_ClusterSelector_ThrowsInvalidOperationException()
    {
        var cluster = new ClusterConfigBuilder(null, new ServiceCollection());
        var act = () => Binder().SetOptions(cluster, new Dictionary<string, object> { ["username"] = "u" });

        act.Should().Throw<InvalidOperationException>().WithMessage("*cluster master*");
    }

    // ── ConfigFromJson wiring ────────────────────────────────────────────────

    [Fact]
    public void ConfigFromJson_NatsAgent_WithAuthOptions_StoresAuthOptsInDefinition()
    {
        const string json = """
        {
            "ClusterId": "C1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "AgentConnections": [{
                "Name": "Nats-1",
                "RepositoryType": "NatsJetStream",
                "ConnectionString": "nats://localhost",
                "ConnectionOptions": {
                    "username": "natsuser",
                    "password": "natssecret"
                }
            }]
        }
        """;

        var b = new ClusterConfigBuilder(null, new ServiceCollection());
        b.ConfigFromJson(json);

        var agentConfig = b.clusterDefinition.AgentConnections[0].AgentAdditionalConnConfig;
        agentConfig.Should().NotBeNull();
        agentConfig
            .TryGetValue<NatsAuthOpts>(NatsJetStreamConfigKey.NamespaceUniqueKey, NatsJetStreamConfigKey.NatsAuthOptsKey)
            .Should().NotBeNull()
            .And.BeEquivalentTo(new { Username = "natsuser", Password = "natssecret" });
    }

    [Fact]
    public void ConfigFromJson_NatsAgent_WithTlsOptions_StoresTlsOptsInDefinition()
    {
        const string json = """
        {
            "ClusterId": "C1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "AgentConnections": [{
                "Name": "Nats-1",
                "RepositoryType": "NatsJetStream",
                "ConnectionString": "nats://localhost",
                "ConnectionOptions": {
                    "tlsCaFile": "/etc/ca.pem",
                    "tlsMode":   "Require"
                }
            }]
        }
        """;

        var b = new ClusterConfigBuilder(null, new ServiceCollection());
        b.ConfigFromJson(json);

        var agentConfig = b.clusterDefinition.AgentConnections[0].AgentAdditionalConnConfig;
        agentConfig.Should().NotBeNull();
        agentConfig
            .TryGetValue<NatsTlsOpts>(NatsJetStreamConfigKey.NamespaceUniqueKey, NatsJetStreamConfigKey.NatsTlsOptsKey)
            .Should().NotBeNull()
            .And.BeEquivalentTo(new { CaFile = "/etc/ca.pem", Mode = TlsMode.Require });
    }

    [Fact]
    public void ConfigFromJson_NatsAgent_WithUnknownConnectionOption_Throws()
    {
        const string json = """
        {
            "ClusterId": "C1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "AgentConnections": [{
                "Name": "Nats-1",
                "RepositoryType": "NatsJetStream",
                "ConnectionString": "nats://localhost",
                "ConnectionOptions": { "badKey": "x" }
            }]
        }
        """;

        var b = new ClusterConfigBuilder(null, new ServiceCollection());
        var act = () => b.ConfigFromJson(json);

        act.Should().Throw<ArgumentException>().WithMessage("*badKey*");
    }

    [Fact]
    public void ConfigFromJson_NatsAgent_CaseInsensitiveConnectionOptionKeys_Work()
    {
        const string json = """
        {
            "ClusterId": "C1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "AgentConnections": [{
                "Name": "Nats-1",
                "RepositoryType": "NatsJetStream",
                "ConnectionString": "nats://localhost",
                "ConnectionOptions": {
                    "Username": "alice",
                    "PASSWORD": "secret"
                }
            }]
        }
        """;

        var b = new ClusterConfigBuilder(null, new ServiceCollection());
        b.ConfigFromJson(json);

        var agentConfig = b.clusterDefinition.AgentConnections[0].AgentAdditionalConnConfig;
        agentConfig.Should().NotBeNull();
        agentConfig
            .TryGetValue<NatsAuthOpts>(NatsJetStreamConfigKey.NamespaceUniqueKey, NatsJetStreamConfigKey.NatsAuthOptsKey)
            .Should().NotBeNull()
            .And.BeEquivalentTo(new { Username = "alice" });
    }
}
