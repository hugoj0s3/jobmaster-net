using JobMaster.Sdk.Abstractions.Ioc;

namespace JobMaster.Sdk.Ioc.Setup.Strategies;

internal static class ConnectionOptionsStrategyFactory
{
    private static readonly Dictionary<string, IConnectionOptionsStrategy> Strategies = Discover();

    public static IConnectionOptionsStrategy Create(string repoType)
        => Strategies.TryGetValue(repoType, out var strategy)
            ? strategy
            : throw new InvalidOperationException($"No IConnectionOptionsStrategy registered for repoType '{repoType}'. Ensure the provider assembly is loaded or remove connectionOptions from the configuration.");

    private static Dictionary<string, IConnectionOptionsStrategy> Discover()
    {
        return AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
            .SelectMany(a =>
            {
                try { return a.GetTypes(); }
                catch { return Array.Empty<Type>(); }
            })
            .Where(t => typeof(IConnectionOptionsStrategy).IsAssignableFrom(t)
                        && !t.IsInterface
                        && !t.IsAbstract
                        && t.GetConstructor(Type.EmptyTypes) != null)
            .Select(t => (IConnectionOptionsStrategy)Activator.CreateInstance(t)!)
            .ToDictionary(s => s.RepoType, s => s);
    }
}
