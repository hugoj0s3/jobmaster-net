using System.Security.Cryptography.X509Certificates;
using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Ioc;

namespace JobMaster.RavenDb;

// JSON-config counterpart to ConfigExtensions.UseRavenDb -- a live X509Certificate2 can't be embedded in
// JSON, so this reads a file path + optional password instead and loads the certificate from disk. Mirrors
// NatsJetStreamConnectionOptionsBinder's shape (auto-discovered by ConnectionOptionsBinderFactory the same
// way).
internal sealed class RavenDbConnectionOptionsBinder : IConnectionOptionsBinder
{
    private static readonly HashSet<string> ClusterKnownKeys = new(StringComparer.OrdinalIgnoreCase)
    {
        "certificateFile", "certificatePassword", "collectionPrefix", "enableDocumentExpiration",
        "documentExpirationFrequencySeconds",
    };

    // No enableDocumentExpiration here -- agent messages never use @expires, matching UseRavenDb's
    // agent-side overload not exposing that parameter either.
    private static readonly HashSet<string> AgentKnownKeys = new(StringComparer.OrdinalIgnoreCase)
    {
        "certificateFile", "certificatePassword", "collectionPrefix",
    };

    public string RepoType => RavenDbRepositoryConstants.RepositoryTypeId;

    public void SetOptions(IClusterConfigSelector selector, IDictionary<string, object> options)
    {
        ValidateKeys(options, ClusterKnownKeys);

        var certificate = BuildCertificate(options);
        if (certificate != null)
        {
            selector.AppendAdditionalConnConfigValue(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CertificateKey, certificate);
        }

        var collectionPrefix = GetString(options, "collectionPrefix");
        if (collectionPrefix != null)
        {
            selector.AppendAdditionalConnConfigValue(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CollectionPrefixKey, collectionPrefix);
        }

        var enableDocumentExpiration = GetString(options, "enableDocumentExpiration");
        if (enableDocumentExpiration != null)
        {
            selector.AppendAdditionalConnConfigValue(
                RavenDbConfigKeys.NamespaceUniqueKey,
                RavenDbConfigKeys.EnableDocumentExpirationKey,
                string.Equals(enableDocumentExpiration, "true", StringComparison.OrdinalIgnoreCase));
        }

        var documentExpirationFrequencySeconds = GetString(options, "documentExpirationFrequencySeconds");
        if (documentExpirationFrequencySeconds != null)
        {
            if (!long.TryParse(documentExpirationFrequencySeconds, out var seconds))
            {
                throw new ArgumentException($"documentExpirationFrequencySeconds must be an integer number of seconds. Received: '{documentExpirationFrequencySeconds}'.");
            }

            selector.AppendAdditionalConnConfigValue(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.DocumentExpirationFrequencyKey, seconds);
        }
    }

    public void SetOptions(IAgentConnectionConfigSelector selector, IDictionary<string, object> options)
    {
        ValidateKeys(options, AgentKnownKeys);

        var certificate = BuildCertificate(options);
        if (certificate != null)
        {
            selector.AppendAdditionalConnConfigValue(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CertificateKey, certificate);
        }

        var collectionPrefix = GetString(options, "collectionPrefix");
        if (collectionPrefix != null)
        {
            selector.AppendAdditionalConnConfigValue(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CollectionPrefixKey, collectionPrefix);
        }
    }

    private static X509Certificate2? BuildCertificate(IDictionary<string, object> options)
    {
        var certificateFile = GetString(options, "certificateFile");
        if (certificateFile is null)
        {
            return null;
        }

        var certificatePassword = GetString(options, "certificatePassword");
        return new X509Certificate2(certificateFile, certificatePassword);
    }

    private static void ValidateKeys(IDictionary<string, object> options, HashSet<string> knownKeys)
    {
        foreach (var key in options.Keys)
        {
            if (!knownKeys.Contains(key))
            {
                throw new ArgumentException($"Unknown RavenDB connection option '{key}'. Supported: {string.Join(", ", knownKeys)}.");
            }
        }
    }

    private static string? GetString(IDictionary<string, object> options, string key)
        => options.TryGetValue(key, out var value) ? value?.ToString() : null;
}
