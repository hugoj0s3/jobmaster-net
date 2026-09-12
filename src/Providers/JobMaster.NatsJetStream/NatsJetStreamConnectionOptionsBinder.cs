using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Ioc;
using NATS.Client.Core;

namespace JobMaster.NatsJetStream;

internal sealed class NatsJetStreamConnectionOptionsBinder : IConnectionOptionsBinder
{
    private static readonly HashSet<string> KnownKeys = new(StringComparer.OrdinalIgnoreCase)
    {
        "username", "password", "token", "credentialsFile", "nkey", "jwt",
        "tlsCertBundleFile", "tlsCertBundleFilePassword",
        "tlsCaFile", "tlsInsecureSkipVerify", "tlsMode"
    };

    public string RepoType => NatsJetStreamConstants.RepositoryTypeId;

    public void SetOptions(IAgentConnectionConfigSelector selector, IDictionary<string, object> options)
    {
        ValidateKeys(options);

        var authOpts = BuildAuthOpts(options);
        if (authOpts is not null)
            selector.AppendAdditionalConnConfigValue(
                NatsJetStreamConfigKey.NamespaceUniqueKey,
                NatsJetStreamConfigKey.NatsAuthOptsKey,
                authOpts);

        var tlsOpts = BuildTlsOpts(options);
        if (tlsOpts is not null)
            selector.AppendAdditionalConnConfigValue(
                NatsJetStreamConfigKey.NamespaceUniqueKey,
                NatsJetStreamConfigKey.NatsTlsOptsKey,
                tlsOpts);
    }

    public void SetOptions(IClusterConfigSelector selector, IDictionary<string, object> options)
        => throw new InvalidOperationException(
            $"NATS JetStream ({NatsJetStreamConstants.RepositoryTypeId}) is not supported as a cluster master repository.");

    private static NatsAuthOpts? BuildAuthOpts(IDictionary<string, object> options)
    {
        var username        = GetString(options, "username");
        var password        = GetString(options, "password");
        var token           = GetString(options, "token");
        var credentialsFile = GetString(options, "credentialsFile");
        var nkey            = GetString(options, "nkey");
        var jwt             = GetString(options, "jwt");

        if (username is null && password is null && token is null && credentialsFile is null && nkey is null && jwt is null)
            return null;

        return new NatsAuthOpts
        {
            Username  = username,
            Password  = password,
            Token     = token,
            CredsFile = credentialsFile,
            NKey      = nkey,
            Jwt       = jwt,
        };
    }

    private static NatsTlsOpts? BuildTlsOpts(IDictionary<string, object> options)
    {
        var certBundleFile         = GetString(options, "tlsCertBundleFile");
        var certBundleFilePassword = GetString(options, "tlsCertBundleFilePassword");
        var caFile                 = GetString(options, "tlsCaFile");
        var insecureSkipVerify     = GetString(options, "tlsInsecureSkipVerify");
        var modeStr                = GetString(options, "tlsMode");

        if (certBundleFile is null && caFile is null && insecureSkipVerify is null && modeStr is null)
            return null;

        var tlsOpts = new NatsTlsOpts
        {
            CertBundleFile         = certBundleFile,
            CertBundleFilePassword = certBundleFilePassword,
            CaFile                 = caFile,
            InsecureSkipVerify     = string.Equals(insecureSkipVerify, "true", StringComparison.OrdinalIgnoreCase),
        };

        if (modeStr is not null)
        {
            if (!Enum.TryParse<TlsMode>(modeStr, ignoreCase: true, out var mode))
                throw new ArgumentException(
                    $"Invalid tlsMode '{modeStr}'. Valid values: {string.Join(", ", Enum.GetNames(typeof(TlsMode)))}.");
            tlsOpts = tlsOpts with { Mode = mode };
        }

        return tlsOpts;
    }

    private static void ValidateKeys(IDictionary<string, object> options)
    {
        foreach (var key in options.Keys)
        {
            if (!KnownKeys.Contains(key))
                throw new ArgumentException(
                    $"Unknown NATS JetStream connection option '{key}'. Supported: {string.Join(", ", KnownKeys)}.");
        }
    }

    private static string? GetString(IDictionary<string, object> options, string key)
        => options.TryGetValue(key, out var value) ? value?.ToString() : null;
}
