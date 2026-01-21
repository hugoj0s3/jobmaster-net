#if JOBMASTER
namespace JobMaster.Internals;
#elif NATJS
namespace JobMaster.NatJetStream.Internals.Utils;
#elif SQLPROV
namespace JobMaster.Sql.Internals.Utils;
#else
namespace JobMaster.Internal.Utils;
#endif

internal static class DictionaryExtensions
{
    internal static T? GetOrDefault<T>(this IDictionary<string, T> dictionary, string key)
    {
        if (dictionary.TryGetValue(key, out var value))
        {
            return (T)value;
        }
        
        return default;
    }
}