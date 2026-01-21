#if JOBMASTER
namespace JobMaster.Internals;
#elif NATJS
namespace JobMaster.NatJetStream.Internals.Utils;
#elif SQLPROV
namespace JobMaster.Sql.Internals.Utils;
#else
namespace JobMaster.Internal.Utils;
#endif

internal static class JobMasterDictionaryUtils
{
    public static IDictionary<string, object?> Merge(params IDictionary<string, object?>[] items)
    {
        var result = new Dictionary<string, object?>();

        foreach (var item in items)
        {
            foreach (var kv in item)
            {
                result[kv.Key] = kv.Value; // last set wins
            }
        }

        return result;
    }
}