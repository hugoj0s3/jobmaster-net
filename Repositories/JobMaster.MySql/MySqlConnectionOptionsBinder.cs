using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.SqlBase;

namespace JobMaster.MySql;

// JSON-config counterpart to ConfigExtensions.UseMySqlForMaster/UseMySqlForAgent's tablePrefix
// parameter. Mirrors RavenDbConnectionOptionsBinder/NatsJetStreamConnectionOptionsBinder's shape
// (auto-discovered by ConnectionOptionsBinderFactory the same way).
internal sealed class MySqlConnectionOptionsBinder : IConnectionOptionsBinder
{
    private static readonly HashSet<string> KnownKeys = new(StringComparer.OrdinalIgnoreCase) { "tablePrefix" };

    public string RepoType => MySqlRepositoryConstants.RepositoryTypeId;

    public void SetOptions(IClusterConfigSelector selector, IDictionary<string, object> options)
    {
        ValidateKeys(options);
        var tablePrefix = GetString(options, "tablePrefix");
        if (tablePrefix != null)
        {
            selector.AppendAdditionalConnConfigValue(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey, tablePrefix);
        }
    }

    public void SetOptions(IAgentConnectionConfigSelector selector, IDictionary<string, object> options)
    {
        ValidateKeys(options);
        var tablePrefix = GetString(options, "tablePrefix");
        if (tablePrefix != null)
        {
            selector.AppendAdditionalConnConfigValue(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey, tablePrefix);
        }
    }

    private static void ValidateKeys(IDictionary<string, object> options)
    {
        foreach (var key in options.Keys)
        {
            if (!KnownKeys.Contains(key))
            {
                throw new ArgumentException($"Unknown MySQL connection option '{key}'. Supported: {string.Join(", ", KnownKeys)}.");
            }
        }
    }

    private static string? GetString(IDictionary<string, object> options, string key)
        => options.TryGetValue(key, out var value) ? value?.ToString() : null;
}
