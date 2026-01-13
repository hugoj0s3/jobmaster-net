namespace JobMaster.Contracts.Extensions;

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