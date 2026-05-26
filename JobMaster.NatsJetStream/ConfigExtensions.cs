using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;
using NATS.Client.Core;

namespace JobMaster.NatsJetStream;

/// <summary>
/// Extension methods for configuring a NATS JetStream agent connection.
/// </summary>
public static class ConfigExtensions
{
    /// <summary>
    /// Configures the agent connection to use NATS JetStream with a pre-built connection string.
    /// </summary>
    /// <param name="agentConfigSelector">The agent connection selector to configure.</param>
    /// <param name="connectionString">NATS connection string (e.g. <c>nats://localhost:4222</c>).</param>
    /// <param name="authOpts">Optional NATS authentication options.</param>
    /// <param name="tlsOpts">Optional NATS TLS options.</param>
    public static IAgentConnectionConfigSelector UseNatsJetStream(
        this IAgentConnectionConfigSelector agentConfigSelector,
        string connectionString,
        NatsAuthOpts? authOpts = null,
        NatsTlsOpts? tlsOpts = null)
    {
        agentConfigSelector.AgentConnString(connectionString);
        agentConfigSelector.AgentRepoType(NatsJetStreamConstants.RepositoryTypeId);

        var advancedSelector = agentConfigSelector;
        if (authOpts is not null)
        {
            advancedSelector.AppendAdditionalConnConfigValue(NatsJetStreamConfigKey.NamespaceUniqueKey, NatsJetStreamConfigKey.NatsAuthOptsKey, authOpts);
        }

        if (tlsOpts is not null)
        {
            advancedSelector.AppendAdditionalConnConfigValue(NatsJetStreamConfigKey.NamespaceUniqueKey, NatsJetStreamConfigKey.NatsTlsOptsKey, tlsOpts);
        }

        return agentConfigSelector;
    }

    /// <summary>
    /// Configures the agent connection to use NATS JetStream with explicit URL and credentials.
    /// </summary>
    /// <param name="agentConfigSelector">The agent connection selector to configure.</param>
    /// <param name="url">NATS server URL (e.g. <c>localhost:4222</c>).</param>
    /// <param name="userName">NATS username.</param>
    /// <param name="password">NATS password.</param>
    /// <param name="authOpts">Optional NATS authentication options.</param>
    /// <param name="connOpts">Optional NATS TLS options.</param>
    public static IAgentConnectionConfigSelector UseNatsJetStream(
        this IAgentConnectionConfigSelector agentConfigSelector,
        string url,
        string userName,
        string password,
        NatsAuthOpts? authOpts = null,
        NatsTlsOpts? connOpts = null)
    {
        var connectionString = BuildConnectionString(url, userName, password);

        return agentConfigSelector.UseNatsJetStream(connectionString, authOpts, connOpts);
    }

    /// <summary>
    /// Configures the agent connection to use NATS JetStream with multiple servers specified as
    /// <c>(url, userName, password)</c> tuples. The servers are joined into a single cluster connection string.
    /// </summary>
    /// <param name="agentConfigSelector">The agent connection selector to configure.</param>
    /// <param name="connectionStrings">Array of <c>(url, userName, password)</c> tuples for each NATS server.</param>
    /// <param name="authOpts">Optional NATS authentication options.</param>
    /// <param name="tlsOpts">Optional NATS TLS options.</param>
    public static IAgentConnectionConfigSelector UseNatsJetStream(
        this IAgentConnectionConfigSelector agentConfigSelector,
        (
            string url,
            string userName,
            string password
        )[] connectionStrings,
        NatsAuthOpts? authOpts = null,
        NatsTlsOpts? tlsOpts = null)
    {
        return agentConfigSelector.UseNatsJetStream(connectionStrings.Select(x => BuildConnectionString(x.url, x.userName, x.password)).ToArray(), authOpts, tlsOpts);
    }

    /// <summary>
    /// Configures the agent connection to use NATS JetStream with multiple pre-built connection strings.
    /// The strings are joined into a single cluster connection string.
    /// </summary>
    /// <param name="agentConfigSelector">The agent connection selector to configure.</param>
    /// <param name="connectionStrings">Array of NATS connection strings.</param>
    /// <param name="authOpts">Optional NATS authentication options.</param>
    /// <param name="tlsOpts">Optional NATS TLS options.</param>
    public static IAgentConnectionConfigSelector UseNatsJetStream(
        this IAgentConnectionConfigSelector agentConfigSelector,
        string[] connectionStrings,
        NatsAuthOpts? authOpts = null,
        NatsTlsOpts? tlsOpts = null)
    {
        var connectionString = string.Join(",", connectionStrings);
        return agentConfigSelector.UseNatsJetStream(connectionString, authOpts, tlsOpts);
    }

    // Normalize base URL
    private static string BuildConnectionString(string url, string userName, string password)
    {
        var baseUrl = url.Trim();
        if (!baseUrl.Contains("://"))
        {
            baseUrl = $"nats://{baseUrl}";
        }

        // Build connection string with optional credentials
        string connectionString;
        if (!string.IsNullOrEmpty(userName))
        {
            var u = Uri.EscapeDataString(userName);
            var p = Uri.EscapeDataString(password ?? string.Empty);
            var sep = baseUrl.IndexOf("://", StringComparison.Ordinal);
            var scheme = baseUrl.Substring(0, sep + 3); // includes ://
            var rest = baseUrl.Substring(sep + 3);
            connectionString = $"{scheme}{u}:{p}@{rest}";
        }
        else
        {
            connectionString = baseUrl;
        }

        return connectionString;
    }
}
