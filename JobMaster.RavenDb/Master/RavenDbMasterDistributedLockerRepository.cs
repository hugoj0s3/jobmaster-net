using JobMaster.RavenDb.Connections;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.Json;

namespace JobMaster.RavenDb.Master;

internal sealed class RavenDbMasterDistributedLockerRepository : JobMasterClusterAwareRepository, IMasterDistributedLockerRepository
{
    // Grace window added on top of the lease's own ExpiresAt before RavenDB's native expiration feature
    // (assumed enabled at the database level -- see RavenDbJobMasterRuntimeSetup once it exists) actually
    // deletes the compare-exchange entry. Lock *validity* never depends on this -- TryLock/IsLocked always
    // compare ExpiresAt client-side and can steal/report-unlocked immediately once a lease is past due,
    // regardless of whether RavenDB's expiration sweep (DeleteFrequencyInSec, not real-time) has caught up
    // yet. @expires is purely a housekeeping backstop for entries nobody ever explicitly released or stole,
    // matching the role the old hand-rolled Timer-based sweep played -- just server-managed now instead.
    private static readonly TimeSpan ZombieLockGracePeriod = TimeSpan.FromDays(2);

    private readonly IDocumentStore store;

    public RavenDbMasterDistributedLockerRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IRavenDbDocumentStoreManager storeManager) : base(clusterConnectionConfig)
    {
        store = storeManager.GetOrCreateStore(clusterConnectionConfig.ConnectionString);
    }

    public override string MasterRepoTypeId => RavenDbRepositoryConstants.RepositoryTypeId;

    private string CompareExchangeKey(string key) => $"lock/{ClusterConnConfig.ClusterId}/{key}";

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
