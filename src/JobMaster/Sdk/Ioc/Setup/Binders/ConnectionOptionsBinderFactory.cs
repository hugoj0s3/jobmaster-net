using JobMaster.Sdk.Abstractions.Ioc;

namespace JobMaster.Sdk.Ioc.Setup.Binders;

internal static class ConnectionOptionsBinderFactory
{
    private static readonly Dictionary<string, IConnectionOptionsBinder> Binders = Discover();

    public static IConnectionOptionsBinder Create(string repoType)
        => Binders.TryGetValue(repoType, out var binder)
            ? binder
            : throw new InvalidOperationException($"No IConnectionOptionsBinder registered for repoType '{repoType}'. Ensure the provider assembly is loaded or remove connectionOptions from the configuration.");

    private static Dictionary<string, IConnectionOptionsBinder> Discover()
    {
        return AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
            .SelectMany(a =>
            {
                try { return a.GetTypes(); }
                catch { return Array.Empty<Type>(); }
            })
            .Where(t => typeof(IConnectionOptionsBinder).IsAssignableFrom(t)
                        && !t.IsInterface
                        && !t.IsAbstract
                        && t.GetConstructor(Type.EmptyTypes) != null)
            .Select(t => (IConnectionOptionsBinder)Activator.CreateInstance(t)!)
            .ToDictionary(s => s.RepoType, s => s);
    }
}
