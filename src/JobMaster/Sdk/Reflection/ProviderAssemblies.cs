using System.Reflection;

namespace JobMaster.Sdk.Reflection;

/// <summary>
/// Loads the first-party provider assembly for a repository type. A provider configured only by its repository type
/// string (e.g. <c>ConfigFromJson</c>) is never referenced in code, so .NET doesn't load its assembly on its own and
/// the provider scans wouldn't find it.
/// </summary>
internal static class ProviderAssemblies
{
    // Kept in sync with each provider's RepositoryTypeId (ProviderAssembliesTests checks it).
    internal static readonly IReadOnlyDictionary<string, string> AssemblyNameByRepositoryType = new Dictionary<string, string>
    {
        ["Postgres"] = "JobMaster.Postgres",
        ["MySql"] = "JobMaster.MySql",
        ["SqlServer"] = "JobMaster.SqlServer",
        ["RavenDB"] = "JobMaster.RavenDB",
        ["NatsJetStream"] = "JobMaster.NatsJetStream",
    };

    /// <summary>
    /// The built-in casing of <paramref name="repositoryType"/> when it matches a built-in type ignoring case
    /// (<c>"postgres"</c> → <c>"Postgres"</c>), else the value unchanged (a third-party provider keeps its own name).
    /// Repository types are matched ignoring case, but what's stored and used as a key is always this casing.
    /// </summary>
    public static string ToKnownRepositoryType(string repositoryType)
    {
        foreach (var known in AssemblyNameByRepositoryType.Keys)
        {
            if (string.Equals(known, repositoryType, StringComparison.OrdinalIgnoreCase))
            {
                return known;
            }
        }

        return repositoryType;
    }

    public static void EnsureLoaded(string repositoryType)
    {
        if (!AssemblyNameByRepositoryType.TryGetValue(repositoryType, out var assemblyName))
        {
            return;
        }

        try
        {
            var assembly = Assembly.Load(new AssemblyName(assemblyName));
            TypeDiscovery.AddAssembly(assembly);
        }
        catch (FileNotFoundException)
        {
            // Package not installed: the caller's "no provider" error names it.
        }
    }

    /// <summary>What to tell the user when no provider registers <paramref name="repositoryType"/>.</summary>
    public static string MissingProviderHint(string repositoryType)
    {
        return AssemblyNameByRepositoryType.TryGetValue(repositoryType, out var assemblyName)
            ? $"Install the {assemblyName} package."
            : $"The built-in repository types are: {string.Join(", ", AssemblyNameByRepositoryType.Keys)}.";
    }
}
