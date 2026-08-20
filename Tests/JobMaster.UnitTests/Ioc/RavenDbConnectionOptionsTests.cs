using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using FluentAssertions;
using JobMaster.RavenDb;
using JobMaster.Sdk.Ioc.Setup;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.UnitTests.Ioc;

public class RavenDbConnectionOptionsTests : IDisposable
{
    private readonly List<string> tempFiles = [];

    static RavenDbConnectionOptionsTests()
    {
        // Force the RavenDb assembly to load before the binder factory initializes its static discovery scan.
        _ = typeof(RavenDbConnectionOptionsBinder);
    }

    public void Dispose()
    {
        foreach (var file in tempFiles)
        {
            try { File.Delete(file); } catch { /* best effort cleanup */ }
        }
    }

    private static RavenDbConnectionOptionsBinder Binder() => new();

    private static ClusterConfigBuilder Cluster() => new(null, new ServiceCollection());

    // Generates a throwaway self-signed cert and writes it to a temp .pfx so the binder has something
    // real to load -- SetOptions actually parses the certificate file immediately, unlike NATS's
    // tlsCertBundleFile (which the binder just stores as a path string for the NATS client to load later).
    private (string FilePath, string Thumbprint) CreateTempCertificateFile(string password)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=jobmaster-ravendb-test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddMinutes(5));

        var path = Path.Combine(Path.GetTempPath(), $"jobmaster-ravendb-test-{Guid.NewGuid():N}.pfx");
        File.WriteAllBytes(path, cert.Export(X509ContentType.Pfx, password));
        tempFiles.Add(path);

        return (path, cert.Thumbprint);
    }

    // ── certificate ─────────────────────────────────────────────────────────

    [Fact]
    public void SetOptions_Cluster_CertificateFile_LoadsCertificate()
    {
        var (path, thumbprint) = CreateTempCertificateFile("pfxpass");
        var cluster = Cluster();

        Binder().SetOptions(cluster, new Dictionary<string, object>
        {
            ["certificateFile"] = path,
            ["certificatePassword"] = "pfxpass",
        });

        var captured = cluster.clusterDefinition.AdditionalConnConfig!
            .TryGetValue<X509Certificate2>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CertificateKey);
        captured.Should().NotBeNull();
        captured!.Thumbprint.Should().Be(thumbprint);
    }

    [Fact]
    public void SetOptions_Agent_CertificateFile_LoadsCertificate()
    {
        var (path, thumbprint) = CreateTempCertificateFile("pfxpass");
        var cluster = Cluster();
        var agentSel = cluster.AddAgentConnectionConfig("Agent-1", "RavenDB", "Urls=http://localhost:8080;Database=Test");

        Binder().SetOptions(agentSel, new Dictionary<string, object>
        {
            ["certificateFile"] = path,
            ["certificatePassword"] = "pfxpass",
        });

        var captured = cluster.clusterDefinition.AgentConnections[0].AgentAdditionalConnConfig!
            .TryGetValue<X509Certificate2>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CertificateKey);
        captured.Should().NotBeNull();
        captured!.Thumbprint.Should().Be(thumbprint);
    }

    [Fact]
    public void SetOptions_Cluster_NoCertificateFile_DoesNotWriteCertificate()
    {
        var cluster = Cluster();

        Binder().SetOptions(cluster, new Dictionary<string, object> { ["collectionPrefix"] = "JM_" });

        cluster.clusterDefinition.AdditionalConnConfig!
            .TryGetValue<X509Certificate2>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CertificateKey)
            .Should().BeNull();
    }

    // ── collection prefix ───────────────────────────────────────────────────

    [Fact]
    public void SetOptions_Cluster_CollectionPrefix_SetsPrefix()
    {
        var cluster = Cluster();

        Binder().SetOptions(cluster, new Dictionary<string, object> { ["collectionPrefix"] = "Custom_" });

        cluster.clusterDefinition.AdditionalConnConfig!
            .TryGetValue<string>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CollectionPrefixKey)
            .Should().Be("Custom_");
    }

    [Fact]
    public void SetOptions_Agent_CollectionPrefix_SetsPrefix()
    {
        var cluster = Cluster();
        var agentSel = cluster.AddAgentConnectionConfig("Agent-1", "RavenDB", "Urls=http://localhost:8080;Database=Test");

        Binder().SetOptions(agentSel, new Dictionary<string, object> { ["collectionPrefix"] = "Custom_" });

        cluster.clusterDefinition.AgentConnections[0].AgentAdditionalConnConfig!
            .TryGetValue<string>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CollectionPrefixKey)
            .Should().Be("Custom_");
    }

    // ── document expiration (master only) ───────────────────────────────────

    [Fact]
    public void SetOptions_Cluster_EnableDocumentExpiration_True_SetsFlag()
    {
        var cluster = Cluster();

        Binder().SetOptions(cluster, new Dictionary<string, object> { ["enableDocumentExpiration"] = "true" });

        cluster.clusterDefinition.AdditionalConnConfig!
            .TryGetValue<bool?>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.EnableDocumentExpirationKey)
            .Should().BeTrue();
    }

    [Fact]
    public void SetOptions_Cluster_EnableDocumentExpiration_False_SetsFlagFalse()
    {
        var cluster = Cluster();

        Binder().SetOptions(cluster, new Dictionary<string, object> { ["enableDocumentExpiration"] = "false" });

        cluster.clusterDefinition.AdditionalConnConfig!
            .TryGetValue<bool?>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.EnableDocumentExpirationKey)
            .Should().BeFalse();
    }

    [Fact]
    public void SetOptions_Cluster_NoEnableDocumentExpirationKey_DoesNotWriteFlag()
    {
        var cluster = Cluster();

        Binder().SetOptions(cluster, new Dictionary<string, object> { ["collectionPrefix"] = "JM_" });

        cluster.clusterDefinition.AdditionalConnConfig!
            .TryGetValue<bool?>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.EnableDocumentExpirationKey)
            .Should().BeNull();
    }

    [Fact]
    public void SetOptions_Agent_EnableDocumentExpiration_ThrowsArgumentException()
    {
        var cluster = Cluster();
        var agentSel = cluster.AddAgentConnectionConfig("Agent-1", "RavenDB", "Urls=http://localhost:8080;Database=Test");

        var act = () => Binder().SetOptions(agentSel, new Dictionary<string, object> { ["enableDocumentExpiration"] = "true" });

        act.Should().Throw<ArgumentException>().WithMessage("*enableDocumentExpiration*");
    }

    [Fact]
    public void SetOptions_Cluster_DocumentExpirationFrequencySeconds_SetsValue()
    {
        var cluster = Cluster();

        Binder().SetOptions(cluster, new Dictionary<string, object> { ["documentExpirationFrequencySeconds"] = "7200" });

        cluster.clusterDefinition.AdditionalConnConfig!
            .TryGetValue<long?>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.DocumentExpirationFrequencyKey)
            .Should().Be(7200);
    }

    [Fact]
    public void SetOptions_Cluster_DocumentExpirationFrequencySeconds_NotAnInteger_ThrowsArgumentException()
    {
        var cluster = Cluster();

        var act = () => Binder().SetOptions(cluster, new Dictionary<string, object> { ["documentExpirationFrequencySeconds"] = "not-a-number" });

        act.Should().Throw<ArgumentException>().WithMessage("*documentExpirationFrequencySeconds*");
    }

    [Fact]
    public void SetOptions_Cluster_NoDocumentExpirationFrequencyKey_DoesNotWriteValue()
    {
        var cluster = Cluster();

        Binder().SetOptions(cluster, new Dictionary<string, object> { ["collectionPrefix"] = "JM_" });

        cluster.clusterDefinition.AdditionalConnConfig!
            .TryGetValue<long?>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.DocumentExpirationFrequencyKey)
            .Should().BeNull();
    }

    [Fact]
    public void SetOptions_Agent_DocumentExpirationFrequencySeconds_ThrowsArgumentException()
    {
        var cluster = Cluster();
        var agentSel = cluster.AddAgentConnectionConfig("Agent-1", "RavenDB", "Urls=http://localhost:8080;Database=Test");

        var act = () => Binder().SetOptions(agentSel, new Dictionary<string, object> { ["documentExpirationFrequencySeconds"] = "7200" });

        act.Should().Throw<ArgumentException>().WithMessage("*documentExpirationFrequencySeconds*");
    }

    // ── key validation ──────────────────────────────────────────────────────

    [Fact]
    public void SetOptions_Cluster_UnknownKey_ThrowsArgumentException()
    {
        var cluster = Cluster();

        var act = () => Binder().SetOptions(cluster, new Dictionary<string, object> { ["unknownProp"] = "x" });

        act.Should().Throw<ArgumentException>().WithMessage("*unknownProp*");
    }

    [Fact]
    public void SetOptions_Agent_UnknownKey_ThrowsArgumentException()
    {
        var cluster = Cluster();
        var agentSel = cluster.AddAgentConnectionConfig("Agent-1", "RavenDB", "Urls=http://localhost:8080;Database=Test");

        var act = () => Binder().SetOptions(agentSel, new Dictionary<string, object> { ["unknownProp"] = "x" });

        act.Should().Throw<ArgumentException>().WithMessage("*unknownProp*");
    }

    [Theory]
    [InlineData("CollectionPrefix", "JM_")]
    [InlineData("ENABLEDOCUMENTEXPIRATION", "true")]
    [InlineData("DOCUMENTEXPIRATIONFREQUENCYSECONDS", "7200")]
    public void SetOptions_Cluster_KeysCaseInsensitive_DoesNotThrow(string key, string value)
    {
        var cluster = Cluster();

        var act = () => Binder().SetOptions(
            cluster,
            new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase) { [key] = value });

        act.Should().NotThrow();
    }

    // ── ConfigFromJson wiring ────────────────────────────────────────────────

    [Fact]
    public void ConfigFromJson_RavenDbCluster_WithConnectionOptions_StoresValuesInDefinition()
    {
        const string json = """
        {
            "ClusterId": "C1",
            "RepoType": "RavenDB",
            "ConnectionString": "Urls=http://localhost:8080;Database=Test",
            "ConnectionOptions": {
                "collectionPrefix": "Custom_",
                "enableDocumentExpiration": "true",
                "documentExpirationFrequencySeconds": "7200"
            }
        }
        """;

        var cluster = Cluster();
        cluster.ConfigFromJson(json);

        var config = cluster.clusterDefinition.AdditionalConnConfig;
        config.Should().NotBeNull();
        config!.TryGetValue<string>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CollectionPrefixKey).Should().Be("Custom_");
        config.TryGetValue<bool?>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.EnableDocumentExpirationKey).Should().BeTrue();
        config.TryGetValue<long?>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.DocumentExpirationFrequencyKey).Should().Be(7200);
    }

    [Fact]
    public void ConfigFromJson_RavenDbAgent_WithCollectionPrefix_StoresInDefinition()
    {
        const string json = """
        {
            "ClusterId": "C1",
            "RepoType": "Postgres",
            "ConnectionString": "Host=localhost;",
            "AgentConnections": [{
                "Name": "RavenDb-1",
                "RepositoryType": "RavenDB",
                "ConnectionString": "Urls=http://localhost:8080;Database=Test",
                "ConnectionOptions": { "collectionPrefix": "Custom_" }
            }]
        }
        """;

        var cluster = Cluster();
        cluster.ConfigFromJson(json);

        var agentConfig = cluster.clusterDefinition.AgentConnections[0].AgentAdditionalConnConfig;
        agentConfig.Should().NotBeNull();
        agentConfig!.TryGetValue<string>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CollectionPrefixKey).Should().Be("Custom_");
    }

    [Fact]
    public void ConfigFromJson_RavenDbCluster_WithUnknownConnectionOption_Throws()
    {
        const string json = """
        {
            "ClusterId": "C1",
            "RepoType": "RavenDB",
            "ConnectionString": "Urls=http://localhost:8080;Database=Test",
            "ConnectionOptions": { "badKey": "x" }
        }
        """;

        var cluster = Cluster();
        var act = () => cluster.ConfigFromJson(json);

        act.Should().Throw<ArgumentException>().WithMessage("*badKey*");
    }
}
