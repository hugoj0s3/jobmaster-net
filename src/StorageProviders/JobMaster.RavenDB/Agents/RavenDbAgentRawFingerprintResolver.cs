using JobMaster.RavenDB.Connections;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CompareExchange;

namespace JobMaster.RavenDB.Agents;

internal sealed class RavenDbAgentRawFingerprintResolver : IAgentRawFingerprintResolver
{
    private readonly IRavenDbDocumentStoreManager storeManager;
    private IDocumentStore store = null!;
    private string prefix = RavenDbConfigKeys.DefaultCollectionPrefix;

    public RavenDbAgentRawFingerprintResolver(IRavenDbDocumentStoreManager storeManager)
    {
        this.storeManager = storeManager;
    }

    public string AgentRepoTypeId => RavenDbRepositoryConstants.RepositoryTypeId;

    private readonly object initializeLock = new();

    public bool IsInitialized { get; private set; }

    public void Initialize(string agentConnectionId, JobMasterAgentProviderConfig agentConnConfig)
    {
        lock (initializeLock)
        {
            if (IsInitialized)
                throw new InvalidOperationException($"{GetType().Name} is already initialized.");

            store = storeManager.GetOrCreateStore(agentConnConfig.ConnectionString, agentConnConfig.GetCertificate(), agentConnConfig.GetRequestTimeout(), agentConnConfig.GetPooledConnectionLifetime(), agentConnConfig.GetPooledConnectionIdleTimeout());
            prefix = agentConnConfig.GetCollectionPrefix();
            IsInitialized = true;
        }
    }

    public async ValueTask<string> GiveYourFingerprintAsync(string clusterId, string agentConnectionId)
    {
        var cxKey = RavenDbDocumentNaming.DocumentId(prefix, clusterId, RavenDbCollectionNames.Fingerprint, agentConnectionId);
        var fingerprint = RepoTypeAndFingerprintUtil.GenerateSingleFingerprint();

        // Create-if-absent: index 0 means "nothing should currently exist for this key". Unlike SQL's
        // duplicate-key exception or NATS KV's create exception, RavenDB reports the conflict via
        // Successful == false -- no exception handling needed to detect a losing race.
        var createResult = await store.Operations.SendAsync(
            new PutCompareExchangeValueOperation<string>(cxKey, fingerprint, 0));
        if (createResult.Successful)
        {
            return fingerprint;
        }

        // Someone else won the race -- defer to whatever they persisted, not our own locally-generated
        // value, which would silently diverge from what's actually stored. A fingerprint never changes
        // once set, so no expiry/CAS-replace branch is needed here (unlike the distributed locker).
        var current = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>(cxKey));
        return current!.Value;
    }
}
