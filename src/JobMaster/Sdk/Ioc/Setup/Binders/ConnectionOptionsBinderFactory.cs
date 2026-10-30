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

        // A provider assembly can load after the first scan, so rescan before giving up.
        binders = Discover();

        return binders.TryGetValue(repoType, out binder)
            ? binder
            : throw new InvalidOperationException($"No IConnectionOptionsBinder registered for repoType '{repoType}'. Ensure the provider assembly is loaded or remove connectionOptions from the configuration.");
    }

    private static Dictionary<string, IConnectionOptionsBinder> Discover()
    {
        return typeof(IConnectionOptionsBinder).GetCachedConcreteTypesOf()
            .Where(t => t.GetConstructor(Type.EmptyTypes) != null)
            .Select(t => (IConnectionOptionsBinder)Activator.CreateInstance(t)!)
            .ToDictionary(s => s.RepoType, s => s);
    }
}
