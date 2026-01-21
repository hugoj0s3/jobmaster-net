namespace JobMaster.Sdk.Abstractions.LocalCache;

public class JobMasterInMemoryCacheItem<T> : JobMasterInMemoryCacheItem
{
    public JobMasterInMemoryCacheItem(DateTime createdAt, DateTime expiresAt, object? valueObj) : base(createdAt, expiresAt, valueObj)
    {
    }

    public T? Value => (T?)ValueObj;
}

public class JobMasterInMemoryCacheItem
{
    public JobMasterInMemoryCacheItem(
        DateTime createdAt,
        DateTime expiresAt,
        object? valueObj)
    {
        CreatedAt = createdAt;
        ExpiresAt = expiresAt;
        ValueObj = valueObj;
    }

    public DateTime CreatedAt { get; private set; }
    public DateTime ExpiresAt { get; private set; }
    public object? ValueObj { get; private set; }
}