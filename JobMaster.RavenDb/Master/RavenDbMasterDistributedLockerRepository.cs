using JobMaster.RavenDb.Connections;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.Json;

namespace JobMaster.RavenDb.Master;

internal sealed class RavenDbMasterDistributedLockerRepository : JobMasterClusterAwareRepository, IMasterDistributedLockerRepository, IDisposable
{
    // Also the cutoff used by the cleanup timer below -- a lock more than this far past its own
    // ExpiresAt is considered a zombie (crashed holder that never released it) and gets deleted.
    private static readonly TimeSpan ZombieLockGracePeriod = TimeSpan.FromDays(2);

    private const int CleanupPageSize = 256;

    private readonly IDocumentStore store;
    private readonly IJobMasterLogger logger;
    private readonly JobMasterLockKeys lockKeys;
    private readonly Timer cleanupTimer;

    public RavenDbMasterDistributedLockerRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IRavenDbDocumentStoreManager storeManager,
        IJobMasterLogger logger) : base(clusterConnectionConfig)
    {
        store = storeManager.GetOrCreateStore(clusterConnectionConfig.ConnectionString, clusterConnectionConfig.GetCertificate());
        this.logger = logger;
        lockKeys = new JobMasterLockKeys(clusterConnectionConfig.ClusterId);

        // Same cadence as SqlMasterDistributedLockerRepository's CleanupTimers -- periodic backstop for
        // locks whose holder crashed without releasing them, independent of the @expires metadata below.
        cleanupTimer = new Timer(_ => SafeCleanupExpiredLocks(), null, TimeSpan.FromHours(2), TimeSpan.FromHours(2));
    }

    public void Dispose()
    {
        cleanupTimer.Dispose();
    }

    public override string MasterRepoTypeId => RavenDbRepositoryConstants.RepositoryTypeId;

    // Compare-exchange keys aren't RavenDB collections, but reusing the same {Prefix}{Name}/{ClusterId}/{Key}
    // scheme as every document ID anyway gives locks the same collision protection (avoid colliding with
    // an unrelated app/deployment sharing this RavenDB database) as everything else this provider stores.
    private string CompareExchangeKey(string key) => RavenDbDocumentNaming.DocumentId(ClusterConnConfig, RavenDbCollectionNames.Lock, key);

    private static IMetadataDictionary ExpiresMetadata(DateTime expiresAt) =>
        new MetadataAsDictionary { [Constants.Documents.Metadata.Expires] = expiresAt.Add(ZombieLockGracePeriod) };

    public string? TryLock(string key, TimeSpan leaseDuration)
    {
        var cxKey = CompareExchangeKey(key);
        var now = DateTime.UtcNow;
        var newToken = JobMasterRandomUtil.NewGuid4().ToString("N");
        var expiresAt = now.Add(leaseDuration);

        // 1) Create-if-absent: index 0 means "nothing should currently exist for this key".
        var createResult = store.Operations.Send(
            new PutCompareExchangeValueOperation<LockPayload>(cxKey, new LockPayload(newToken, expiresAt), 0, ExpiresMetadata(expiresAt)));
        if (createResult.Successful)
        {
            return newToken;
        }

        // 2) Someone already holds the key -- read the current value/index to see if it's expired.
        var current = store.Operations.Send(new GetCompareExchangeValueOperation<LockPayload>(cxKey));
        if (current == null || current.Value.ExpiresAt > now)
        {
            return null;
        }

        // 3) Expired -- CAS-replace using the current index (single attempt, matches SQL's semantics).
        var replaceResult = store.Operations.Send(
            new PutCompareExchangeValueOperation<LockPayload>(cxKey, new LockPayload(newToken, expiresAt), current.Index, ExpiresMetadata(expiresAt)));
        return replaceResult.Successful ? newToken : null;
    }

    public bool ReleaseLock(string key, string lockToken)
    {
        var cxKey = CompareExchangeKey(key);
        var current = store.Operations.Send(new GetCompareExchangeValueOperation<LockPayload>(cxKey));
        if (current == null || current.Value.Token != lockToken)
        {
            return false;
        }

        var deleteResult = store.Operations.Send(new DeleteCompareExchangeValueOperation<LockPayload>(cxKey, current.Index));
        return deleteResult.Successful;
    }

    public bool IsLocked(string key)
    {
        var current = store.Operations.Send(new GetCompareExchangeValueOperation<LockPayload>(CompareExchangeKey(key)));
        return current != null && current.Value.ExpiresAt > DateTime.UtcNow;
    }

    public bool ForceReleaseLock(string key)
    {
        var cxKey = CompareExchangeKey(key);
        var current = store.Operations.Send(new GetCompareExchangeValueOperation<LockPayload>(cxKey));
        if (current == null)
        {
            return false;
        }

        var deleteResult = store.Operations.Send(new DeleteCompareExchangeValueOperation<LockPayload>(cxKey, current.Index));
        return deleteResult.Successful;
    }

    // Mirrors SqlMasterDistributedLockerRepository.SafeCleanupExpiredLocks -- the LockCleanupLock guards
    // against every worker instance running this at once. Compare-exchange values aren't queryable by
    // RQL, so zombies are found by paging GetCompareExchangeValuesOperation over this cluster's lock
    // key prefix instead of a single DELETE WHERE.
    private void SafeCleanupExpiredLocks()
    {
        string? lockToken = null;
        try
        {
            lockToken = TryLock(lockKeys.LockCleanupLock(), TimeSpan.FromHours(2));
            if (lockToken == null) return;

            var cutoff = DateTime.UtcNow.Subtract(ZombieLockGracePeriod);
            var keyPrefix = RavenDbDocumentNaming.DocumentId(ClusterConnConfig, RavenDbCollectionNames.Lock, string.Empty);
            var deletedCount = 0;
            var start = 0;

            while (true)
            {
                var page = store.Operations.Send(new GetCompareExchangeValuesOperation<LockPayload>(keyPrefix, start, CleanupPageSize));
                if (page.Count == 0) break;

                foreach (var entry in page.Values)
                {
                    if (entry.Value.ExpiresAt < cutoff)
                    {
                        var deleteResult = store.Operations.Send(new DeleteCompareExchangeValueOperation<LockPayload>(entry.Key, entry.Index));
                        if (deleteResult.Successful) deletedCount++;
                    }
                }

                if (page.Count < CleanupPageSize) break;
                start += CleanupPageSize;
            }

            if (deletedCount > 0)
            {
                logger.Warn(
                    $"Zombie Locks Detected: Cleanup removed {deletedCount} lock(s) that were expired more than {ZombieLockGracePeriod.TotalHours}h. Investigate worker stability.",
                    JobMasterLogCategory.Cluster,
                    ClusterConnConfig.ClusterId);
            }
        }
        catch (Exception e)
        {
            logger.Error("Failed to cleanup expired locks", JobMasterLogCategory.Cluster, ClusterConnConfig.ClusterId, exception: e);
            if (!string.IsNullOrEmpty(lockToken))
            {
                ReleaseLock(lockKeys.LockCleanupLock(), lockToken!);
            }
        }
    }

    // Plain class, not a record -- record positional syntax needs IsExternalInit, unavailable on
    // netstandard2.0 without a polyfill, and no other provider in this repo uses records for this reason.
    private sealed class LockPayload
    {
        public LockPayload(string token, DateTime expiresAt)
        {
            Token = token;
            ExpiresAt = expiresAt;
        }

        public string Token { get; }
        public DateTime ExpiresAt { get; }
    }
}
