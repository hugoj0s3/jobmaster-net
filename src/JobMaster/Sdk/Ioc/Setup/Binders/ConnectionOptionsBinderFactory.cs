using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Reflection;

namespace JobMaster.Sdk.Ioc.Setup.Binders;

internal static class ConnectionOptionsBinderFactory
{
    private static volatile Dictionary<string, IConnectionOptionsBinder> binders = Discover();

    public static IConnectionOptionsBinder Create(string repoType)
    {
        if (binders.TryGetValue(repoType, out var binder))
            return binder;

        // The provider assembly may not be loaded yet (e.g. ConfigFromJson names it only by its repo type), so load it
        // and rescan before giving up.
        ProviderAssemblies.EnsureLoaded(repoType);
        binders = Discover();

        return binders.TryGetValue(repoType, out binder)
            ? binder
            : throw new InvalidOperationException($"No IConnectionOptionsBinder registered for repoType '{repoType}'. {ProviderAssemblies.MissingProviderHint(repoType)}");
    }

    private static Dictionary<string, IConnectionOptionsBinder> Discover()
    {
        return typeof(IConnectionOptionsBinder).GetCachedConcreteTypesOf()
            .Where(t => t.GetConstructor(Type.EmptyTypes) != null)
            .Select(t => (IConnectionOptionsBinder)Activator.CreateInstance(t)!)
            .ToDictionary(s => s.RepoType, s => s);
    }
}
