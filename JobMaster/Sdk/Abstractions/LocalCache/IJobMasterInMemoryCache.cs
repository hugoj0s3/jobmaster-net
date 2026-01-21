using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.LocalCache;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IJobMasterInMemoryCache
{
    JobMasterInMemoryCacheItem<T>? Get<T>(string key);
    void Set<T>(string key, T value, TimeSpan? durationToExpire = null);
    bool Exists(string key);
    void Remove(string key);
}