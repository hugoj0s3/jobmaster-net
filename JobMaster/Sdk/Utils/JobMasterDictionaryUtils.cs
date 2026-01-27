namespace JobMaster.Sdk.Utils;

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