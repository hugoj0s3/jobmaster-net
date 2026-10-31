namespace JobMaster.Sdk.Utils;

internal static class JobMasterDictionaryUtils
{
    /// <summary>
    /// Merges <paramref name="items"/> in order, ignoring key case: the last value wins, while a key keeps its first
    /// spelling and position.
    /// </summary>
    public static IDictionary<string, object?> Merge(params IDictionary<string, object?>[] items)
    {
        var result = NewIgnoreCase();

        foreach (var item in items)
        {
            foreach (var kv in item)
            {
                result[kv.Key] = kv.Value;
            }
        }

        return result;
    }

    public static Dictionary<string, object?> NewIgnoreCase() => new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// An ignore-case copy of <paramref name="source"/> (never the same instance): the last value wins, a key keeps
    /// its first spelling and position.
    /// </summary>
    public static Dictionary<string, object?> ToCaseInsensitive(this IDictionary<string, object?> source)
    {
        var result = NewIgnoreCase();
        foreach (var kv in source)
        {
            result[kv.Key] = kv.Value;
        }

        return result;
    }
}
