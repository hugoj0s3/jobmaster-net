using System.Collections.Concurrent;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Ioc.Definitions;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace JobMaster.NatJetStreams;

using System.Threading;
using System.Threading.Tasks;
using NATS.Client.JetStream.Models;
using Nito.AsyncEx;

internal sealed class NatJetStreamConnector
#if NET8_0_OR_GREATER
    : IAsyncDisposable
#else
    : System.IDisposable
#endif
{
    
    private sealed class Entry
    {
        public NatsConnection Nats { get; set; } = null!;
        public NatsJSContext Js { get; set; } = null!;
        public string StreamName { get; set; } = null!;
        public volatile bool StreamEnsured;
        public SemaphoreSlim Lock { get; } = new(1, 1);
        public ConcurrentDictionary<string, INatsJSConsumer> Consumers { get; } = new();
    }

    private readonly ConcurrentDictionary<string, Entry> entries = new();
    private static readonly SemaphoreSlim GlobalSetupLock = new(1, 1);
    private static readonly NatJetStreamConnector Instance = new();

    private NatJetStreamConnector() { }

    // Static facade to preserve current call sites
    public static (NatsConnection nats, NatsJSContext jsContext, string streamName) GetOrCreateConnection(JobMasterAgentConnectionConfig config)
        => Instance.GetOrCreateConnectionInternal(config);

    public static Task EnsureStreamAsync(JobMasterAgentConnectionConfig config)
        => Instance.EnsureStreamInternalAsync(config);
    
    public static ValueTask<INatsJSConsumer> GetOrCreateConsumerAsync(JobMasterAgentConnectionConfig config, string fullBucketAddressId)
        => Instance.GetOrCreateConsumerInternalAsync(config, fullBucketAddressId);
    
    public static ValueTask<INatsJSConsumer> CreateOrUpdateConsumerAsync(JobMasterAgentConnectionConfig config, string fullBucketAddressId, int actualBatchSize, CancellationToken ct)
        => Instance.CreateOrUpdateConsumerInternalAsync(config, fullBucketAddressId, actualBatchSize, ct);

#if NET8_0_OR_GREATER
    public static ValueTask DisposeAllAsync() => Instance.DisposeAsync();
    public static ValueTask DisposeOneAsync(string agentConnectionId) => Instance.DisposeOneInternalAsync(agentConnectionId);
#else
    public static void DisposeAll() => Instance.Dispose();
    public static void DisposeOne(string agentConnectionId) => Instance.DisposeOneInternal(agentConnectionId);
#endif

    private (NatsConnection nats, NatsJSContext jsContext, string streamName) GetOrCreateConnectionInternal(JobMasterAgentConnectionConfig config)
    {
        var key = config.Id;
        if (entries.TryGetValue(key, out var existing))
        {
            return (existing.Nats, existing.Js, existing.StreamName);
        }

        // Global serialization for connection create; try once up to 15s, then proceed unlocked
        var gotGlobal = GlobalSetupLock.Wait(TimeSpan.FromSeconds(15));
        try
        {
            if (entries.TryGetValue(key, out existing))
            {
                return (existing.Nats, existing.Js, existing.StreamName);
            }

            NatsAuthOpts? authOpts = config.AdditionalConnConfig.TryGetValue<NatsAuthOpts>(NatJetStreamConfigKey.NamespaceUniqueKey, NatJetStreamConfigKey.NatsAuthOptsKey);
            NatsTlsOpts? tlsOpts = config.AdditionalConnConfig.TryGetValue<NatsTlsOpts>(NatJetStreamConfigKey.NamespaceUniqueKey, NatJetStreamConfigKey.NatsTlsOptsKey);

            // Build options (TLS/auth customizations can be added later)
            var url = config.ConnectionString;
            var clientName = NatJetStreamUtils.GetStreamName(config.Id);
            var streamNameInit = NatJetStreamUtils.GetStreamName(config.Id);

            var opts = NatsOpts.Default with { Url = url, Name = clientName };
            if (authOpts is not null)
            {
                opts = opts with { AuthOpts = authOpts };
            }
            if (tlsOpts is not null)
            {
                opts = opts with { TlsOpts = tlsOpts };
            }

            var natsConn = new NatsConnection(opts);
            var jsCtx = new NatsJSContext(natsConn);

            var entry = new Entry
            {
                Nats = natsConn,
                Js = jsCtx,
                StreamName = streamNameInit,
                StreamEnsured = false,
            };

            entries[key] = entry;
            return (entry.Nats, entry.Js, entry.StreamName);
        }
        finally
        {
            if (gotGlobal) { try { GlobalSetupLock.Release(); } catch { } }
        }
    }

    private async Task EnsureStreamInternalAsync(JobMasterAgentConnectionConfig config)
    {
        GetOrCreateConnectionInternal(config);
        var e = entries[config.Id];
        if (e.StreamEnsured) return;

        // Serialize stream ensure globally and per-entry; try once each and proceed regardless
        var gotGlobal = await GlobalSetupLock.WaitAsync(TimeSpan.FromSeconds(15));
        try
        {
            var gotEntry = await e.Lock.WaitAsync(TimeSpan.FromSeconds(10));

            if (e.StreamEnsured) return;
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            try
            {
                await e.Js.GetStreamAsync(e.StreamName, cancellationToken: cts.Token);
            }
            catch (NatsJSApiException)
            {
                var cfg = new StreamConfig(e.StreamName, new[] { $"{NatJetStreamConstants.Prefix}{config.Id}.>" })
                {
                    DuplicateWindow = TimeSpan.FromMinutes(2),
                };
                await e.Js.CreateStreamAsync(cfg, cts.Token);
            }
            e.StreamEnsured = true;
        }
        finally
        {
            try { e.Lock.Release(); } catch { }
            if (gotGlobal) { try { GlobalSetupLock.Release(); } catch { } }
        }
    }
    
    private async ValueTask<INatsJSConsumer> CreateOrUpdateConsumerInternalAsync(
        JobMasterAgentConnectionConfig config, 
        string fullBucketAddressId,
        int? actualBatchSize = null,
        CancellationToken ct = default)
    {
        // Ensure connection and stream are available
        _ = GetOrCreateConnectionInternal(config);
        await EnsureStreamInternalAsync(config);
        var entry = entries[config.Id];

        // 1. Check if the consumer is already cached to avoid redundant network calls
        if (entry.Consumers.TryGetValue(fullBucketAddressId, out var existing))
        {
            return existing;
        }

        // Serialize create/update GLOBALLY to avoid parallel API calls across entries; try once and proceed regardless
        var gotGlobal = await GlobalSetupLock.WaitAsync(TimeSpan.FromSeconds(15));

        try
        {
            // Also serialize per-entry to limit contention inside an agent; try once and proceed regardless
            var gotEntry = await entry.Lock.WaitAsync(TimeSpan.FromSeconds(10));

            try
            {
                // Double-check after acquiring the locks
                if (entry.Consumers.TryGetValue(fullBucketAddressId, out existing))
                {
                    return existing;
                }

                // 2. Handle the missing CancellationToken (for Repository/Dispatcher calls)
                // We create a local 10s timeout to ensure the boot process doesn't hang if NATS is unreachable.
                using var cts = ct == default ? new CancellationTokenSource(TimeSpan.FromSeconds(10)) : null;
                var token = cts?.Token ?? ct;

                var consumerName = NatJetStreamUtils.GetConsumerName(fullBucketAddressId);
                var subject = NatJetStreamUtils.GetSubjectName(config.Id, fullBucketAddressId);

                actualBatchSize ??= new WorkerDefinition().BatchSize; // Get default batch size if not specified.

                var consumerConfig = new ConsumerConfig(consumerName)
                {
                    FilterSubject = subject,
                    DurableName = consumerName,
                    AckPolicy = ConsumerConfigAckPolicy.Explicit,
                    DeliverPolicy = ConsumerConfigDeliverPolicy.All,
                    MaxAckPending = actualBatchSize.Value, 
                    AckWait = NatJetStreamConstants.ConsumerAckWait,
                    MaxDeliver = NatJetStreamConstants.MaxDeliver,
                };

                // 3. API call to NATS JetStream to create/update the durable consumer
                var consumer = await entry.Js.CreateOrUpdateConsumerAsync(entry.StreamName, consumerConfig, token);
            
                // 4. Store the consumer handle in the cache for future access (e.g., by the Runner)
                entry.Consumers[fullBucketAddressId] = consumer;
            
                return consumer;
            }
            finally
            {
                if (gotEntry) { try { entry.Lock.Release(); } catch { } }
            }
        }
        finally
        {
            if (gotGlobal) { try { GlobalSetupLock.Release(); } catch { } }
        }
    }
    
    private async ValueTask<INatsJSConsumer> GetOrCreateConsumerInternalAsync(JobMasterAgentConnectionConfig config, string fullBucketAddressId)
    {
        // Return cached consumer if present
        var entry = entries[config.Id];
        if (entry.Consumers.TryGetValue(fullBucketAddressId, out var existing))
        {
            return existing;
        }

        return await CreateOrUpdateConsumerInternalAsync(config, fullBucketAddressId);
    } 

#if NET8_0_OR_GREATER
    public async ValueTask DisposeAsync()
    {
        foreach (var kv in entries)
        {
            try { await kv.Value.Nats.DisposeAsync(); } catch { }
            try { kv.Value.Lock.Dispose(); } catch { }
        }
        entries.Clear();
    }

    private async ValueTask DisposeOneInternalAsync(string agentConnectionId)
    {
        if (!entries.TryRemove(agentConnectionId, out var e))
            return;

        try { await e.Nats.DisposeAsync(); } catch { }
        try { e.Lock.Dispose(); } catch { }
    }
#else
    public void Dispose()
    {
        foreach (var kv in entries)
        {
            try { AsyncContext.Run(async () => await kv.Value.Nats.DisposeAsync()); } catch { }
            try { kv.Value.Lock.Dispose(); } catch { }
        }
        entries.Clear();
    }

    private void DisposeOneInternal(string agentConnectionId)
    {
        if (!entries.TryRemove(agentConnectionId, out var e))
            return;

        try { AsyncContext.Run(async () => await e.Nats.DisposeAsync()); } catch { }
        try { e.Lock.Dispose(); } catch { }
    }
#endif
}
