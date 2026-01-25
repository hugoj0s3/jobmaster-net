using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;
using NATS.Client.Core;

namespace JobMaster.NatsJetStream;

public static class ConfigExtensions
{
    public static IAgentConnectionConfigSelector UseNatJetStream(
        this IAgentConnectionConfigSelector agentConfigSelector, 
        string connectionString,
        NatsAuthOpts? authOpts = null,
        NatsTlsOpts? tlsOpts = null)
    {
        ((IAgentConnectionConfigSelectorAdvanced)agentConfigSelector).AgentConnString(connectionString);
        ((IAgentConnectionConfigSelectorAdvanced)agentConfigSelector).AgentRepoType(NatJetStreamConstants.RepositoryTypeId);
        
        var advancedSelector = (IAgentConnectionConfigSelectorAdvanced) agentConfigSelector;
        if (authOpts is not null)
        {
            advancedSelector.AppendAdditionalConnConfigValue(NatJetStreamConfigKey.NamespaceUniqueKey, NatJetStreamConfigKey.NatsAuthOptsKey, authOpts);
        }
        
        if (tlsOpts is not null)
        {
            advancedSelector.AppendAdditionalConnConfigValue(NatJetStreamConfigKey.NamespaceUniqueKey, NatJetStreamConfigKey.NatsTlsOptsKey, tlsOpts);
        }
        
        return agentConfigSelector;
    }
    
    public static IAgentConnectionConfigSelector UseNatJetStream(
        this IAgentConnectionConfigSelector agentConfigSelector, 
        string url, 
        string userName, 
        string password,
        NatsAuthOpts? authOpts = null,
        NatsTlsOpts? connOpts = null)
    {
        var connectionString = BuildConnectionString(url, userName, password);

        return agentConfigSelector.UseNatJetStream(connectionString, authOpts, connOpts);
    }
    
    public static IAgentConnectionConfigSelector UseNatJetStream(
        this IAgentConnectionConfigSelector agentConfigSelector, 
        (
            string url, 
            string userName, 
            string password
        )[] connectionStrings,
        NatsAuthOpts? authOpts = null,
        NatsTlsOpts? tlsOpts = null)
    {
        return agentConfigSelector.UseNatJetStream( connectionStrings.Select(x => BuildConnectionString(x.url, x.userName, x.password)).ToArray(), authOpts, tlsOpts);
    }

    public static IAgentConnectionConfigSelector UseNatJetStream(
        this IAgentConnectionConfigSelector agentConfigSelector, 
        string[] connectionStrings,
        NatsAuthOpts? authOpts = null,
        NatsTlsOpts? tlsOpts = null)
    {
        var connectionString = string.Join(",", connectionStrings);
        return agentConfigSelector.UseNatJetStream(connectionString, authOpts, tlsOpts);
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

