namespace JobMaster.Sdk.Abstractions.LocalCache;

internal interface IJobMasterInMemoryCache
{
    JobMasterInMemoryCacheItem<T>? Get<T>(string key);
    JobMasterInMemoryCacheItem<T> Set<T>(string key, T value, TimeSpan? durationToExpire = null);

    public JobMasterInMemoryCacheItem<T> GetOrSet<T>(
        string key,
        Func<T> valueFactory,
        TimeSpan? valueFactoryLockDuration = null,
        TimeSpan? durationToExpire = null);

    public Task<JobMasterInMemoryCacheItem<T>> GetOrSetAsync<T>(
        string key,
        Func<Task<T>> valueFactory,
        TimeSpan? valueFactoryLockDuration = null,
        TimeSpan? durationToExpire = null);

    bool Exists(string key);
    void Remove(string key);
}