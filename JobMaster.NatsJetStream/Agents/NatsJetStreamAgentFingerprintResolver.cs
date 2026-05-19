using System.Text;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Utils;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;

namespace JobMaster.NatsJetStream.Agents;

internal sealed class NatsJetStreamAgentFingerprintResolver : IAgentFingerprintResolver
{
    private NatsConnection? natsConnection;
    private INatsJSContext? kvContext;
    private JobMasterAgentConnectionConfig? config;
    private const string KvSuffixStoreName = "agent_fingerprints";
    public string AgentRepoTypeId => NatsJetStreamConstants.RepositoryTypeId;

    public async ValueTask<string> GiveYourFingerprintAsync(string clusterId, string agentConnectionId)
    {
        if (natsConnection == null)
            throw new InvalidOperationException("NatsJetStreamAgentFingerprintResolver not initialized. Call Initialize first.");

        var kvStore = await EnsureKvStoreAsync(clusterId);

        var key = agentConnectionId;
        key = key.Replace("-", "_").Replace(".", "_").Replace(":", "_");

        // 1. Try get existing fingerprint
        try
        {
            var entry = await kvStore!.GetEntryAsync<string>(key);
            if (!string.IsNullOrWhiteSpace(entry.Value))
            {
                return entry.Value!;
            }
        }
        catch (NatsKVKeyNotFoundException)
        {
            // Key doesn't exist, continue to create
        }

        // 2. Generate new fingerprint
        var fingerprint = JobMasterRandomUtil.NewGuid4().ToString("N");

        // 3. Try to create (insert only if missing)
        try
        {
            await kvStore!.CreateAsync(key, fingerprint);
            return fingerprint;
        }
        catch (NatsKVCreateException)
        {
            // Key already exists due to race condition
        }
        catch (NatsKVWrongLastRevisionException)
        {
            // Key already exists due to race condition
        }

        // 4. Read again and return whatever is stored now
        try
        {
            var entry = await kvStore!.GetEntryAsync<string>(key);
            if (!string.IsNullOrWhiteSpace(entry.Value))
            {
                return entry.Value!;
            }
        }
        catch (NatsKVKeyNotFoundException)
        {
            // Shouldn't happen, but fallback to generated fingerprint
        }

        return fingerprint;
    }
    
    public void Initialize(JobMasterAgentConnectionConfig agentConnConfig)
    {
        this.config = agentConnConfig;
        (natsConnection, kvContext, _) = NatsJetStreamConnector.GetOrCreateConnection(agentConnConfig);
    }

    private async Task<INatsKVStore> EnsureKvStoreAsync(string clusterId)
    {
        var natsKvContext = new NatsKVContext(kvContext!);

        try
        {
            // Try to get existing KV store
            var kvStore = await natsKvContext.GetStoreAsync($"{clusterId.Replace("-", "_")}_{KvSuffixStoreName}");
            return kvStore;
        }
        catch
        {
            // If it doesn't exist, create it
            var kvConfig = new NatsKVConfig($"{clusterId.Replace("-", "_")}_{KvSuffixStoreName}")
            {
                History = 1,
                MaxBytes = 1024 * 1024 * 10
            };

            var kvStore = await natsKvContext.CreateStoreAsync(kvConfig);
            return kvStore;
        }
    }
}