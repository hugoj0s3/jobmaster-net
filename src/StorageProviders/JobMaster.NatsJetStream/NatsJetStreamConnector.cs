using System.Collections.Concurrent;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Background.Cancellation;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Master;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace JobMaster.NatsJetStream;

using System.Threading;
using System.Threading.Tasks;
using NATS.Client.JetStream.Models;
using Nito.AsyncEx;

internal sealed class NatsJetStreamConnector
#if NET6_0_OR_GREATER
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
    private static readonly NatsJetStreamConnector Instance = new();

    private NatsJetStreamConnector() { }

    private static string GetEntryKey(string agentConnectionId, JobMasterAgentProviderConfig config)
        => $"{agentConnectionId}::{config.ConnectionString}";

    // Static facade to preserve current call sites
    public static (NatsConnection nats, NatsJSContext jsContext, string streamName) GetOrCreateConnection(string agentConnectionId, JobMasterAgentProviderConfig config)
        => Instance.GetOrCreateConnectionInternal(agentConnectionId, config);

    public static Task EnsureStreamAsync(string agentConnectionId, JobMasterAgentProviderConfig config, IJobMasterLogger logger, CancellationToken ct = default)
        => Instance.EnsureStreamInternalAsync(agentConnectionId, config, logger, ct);

    private static JobMasterStopControlFactory StopControlFactory => JobMasterRuntimeSingleton.Instance.StopControlFactory;
    
    public static ValueTask<INatsJSConsumer> GetOrCreateConsumerAsync(string agentConnectionId, JobMasterAgentProviderConfig config, string fullBucketAddressId)
        => Instance.GetOrCreateConsumerInternalAsync(agentConnectionId, config, fullBucketAddressId);
    
    public static ValueTask<INatsJSConsumer> CreateOrUpdateConsumerAsync(string agentConnectionId, JobMasterAgentProviderConfig config, string fullBucketAddressId, int actualBatchSize, TimeSpan bucketBufferLeadTime, TimeSpan transientThreshold, CancellationToken ct)
        => Instance.CreateOrUpdateConsumerInternalAsync(agentConnectionId, config, fullBucketAddressId, actualBatchSize, bucketBufferLeadTime, transientThreshold, ct);

#if NET6_0_OR_GREATER
    public static ValueTask DisposeAllAsync() => Instance.DisposeAsync();
    public static ValueTask DisposeOneAsync(string agentConnectionId) => Instance.DisposeOneInternalAsync(agentConnectionId);
#else
    public static void DisposeAll() => Instance.Dispose();
    public static void DisposeOne(string agentConnectionId) => Instance.DisposeOneInternal(agentConnectionId);
#endif

    private (NatsConnection nats, NatsJSContext jsContext, string streamName) GetOrCreateConnectionInternal(string agentConnectionId, JobMasterAgentProviderConfig config)
    {
        var key = GetEntryKey(agentConnectionId, config);
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

            NatsAuthOpts? authOpts = config.AdditionalConnConfig.TryGetValue<NatsAuthOpts>(NatsJetStreamConfigKey.NamespaceUniqueKey, NatsJetStreamConfigKey.NatsAuthOptsKey);
            NatsTlsOpts? tlsOpts = config.AdditionalConnConfig.TryGetValue<NatsTlsOpts>(NatsJetStreamConfigKey.NamespaceUniqueKey, NatsJetStreamConfigKey.NatsTlsOptsKey);

            // Build options (TLS/auth customizations can be added later)
            var url = NormalizeNatsUrl(config.ConnectionString);
            var clientName = NatsJetStreamUtils.GetStreamName(agentConnectionId);
            var streamNameInit = NatsJetStreamUtils.GetStreamName(agentConnectionId);

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

    // Creates the stream, or brings an existing one to the configured MaxAge / replicas (last process to start wins).
    // logger is null only from the consumer path, which runs after its callers ensured the stream.
    private async Task EnsureStreamInternalAsync(string agentConnectionId, JobMasterAgentProviderConfig config, IJobMasterLogger? logger, CancellationToken ct)
    {
        GetOrCreateConnectionInternal(agentConnectionId, config);
        var e = entries[GetEntryKey(agentConnectionId, config)];
        if (e.StreamEnsured) return;

        var maxAge = config.AdditionalConnConfig.TryGetValue<TimeSpan?>(NatsJetStreamConfigKey.NamespaceUniqueKey, NatsJetStreamConfigKey.StreamMaxAgeKey)
                     ?? NatsJetStreamConstants.MaxAge;
        var numReplicas = config.AdditionalConnConfig.TryGetValue<int?>(NatsJetStreamConfigKey.NamespaceUniqueKey, NatsJetStreamConfigKey.StreamNumReplicasKey)
                          ?? NatsJetStreamConstants.DefaultNumReplicas;

        // Serialize stream ensure globally and per-entry; try once each and proceed regardless
        var gotGlobal = await GlobalSetupLock.WaitAsync(TimeSpan.FromSeconds(15));
        var gotEntry = false;
        try
        {
            gotEntry = await e.Lock.WaitAsync(TimeSpan.FromSeconds(10));

            if (e.StreamEnsured) return;
            // The whole setup (look up, create or update): with the caller, at most 10 s so a boot doesn't hang.
            using var cts = StopControlFactory.ForOperation(ct, TimeSpan.FromSeconds(10));
            INatsJSStream? existing = null;
            try
            {
                existing = await e.Js.GetStreamAsync(e.StreamName, cancellationToken: cts.Token);
            }
            catch (NatsJSApiException)
            {
                var cfg = new StreamConfig(e.StreamName, new[] { $"{NatsJetStreamConstants.Prefix}{agentConnectionId}.>" })
                {
                    DuplicateWindow = NatsJetStreamConstants.DuplicateWindow,
                    MaxAge = maxAge,
                    NumReplicas = numReplicas,
                };
                await e.Js.CreateStreamAsync(cfg, cts.Token);
            }

            if (existing is not null)
            {
                var current = existing.Info.Config;
                if (current.MaxAge != maxAge || current.NumReplicas != numReplicas)
                {
                    await e.Js.UpdateStreamAsync(current with { MaxAge = maxAge, NumReplicas = numReplicas }, cts.Token);
                    logger?.Info(
                        $"NATS stream {e.StreamName} updated: MaxAge {current.MaxAge} -> {maxAge}, replicas {current.NumReplicas} -> {numReplicas}.",
                        JobMasterLogCategory.Cluster);
                }
            }

            e.StreamEnsured = true;
        }
        finally
        {
            // Only a lock this call holds: releasing one it timed out on would free another caller's.
            if (gotEntry) { try { e.Lock.Release(); } catch { } }
            if (gotGlobal) { try { GlobalSetupLock.Release(); } catch { } }
        }
    }
    
    private async ValueTask<INatsJSConsumer> CreateOrUpdateConsumerInternalAsync(
        string agentConnectionId, JobMasterAgentProviderConfig config,
        string fullBucketAddressId,
        int? bufferSize = null,
        TimeSpan? bucketBufferLeadTime = null,
        TimeSpan? transientThreshold = null,
        CancellationToken ct = default)
    {
        // Ensure connection and stream are available
        _ = GetOrCreateConnectionInternal(agentConnectionId, config);
        await EnsureStreamInternalAsync(agentConnectionId, config, logger: null, ct);
        var entry = entries[GetEntryKey(agentConnectionId, config)];

        // 1. Check if the consumer is already cached to avoid redundant network calls
        if (entry.Consumers.TryGetValue(fullBucketAddressId, out var existing))
        {
            return existing;
        }

        // Serialize create/update GLOBALLY to avoid parallel API calls across entries; try once and proceed regardless
        var gotGlobal = await GlobalSetupLock.WaitAsync(TimeSpan.FromSeconds(15), ct);

        try
        {
            // Also serialize per-entry to limit contention inside an agent; try once and proceed regardless
            var gotEntry = await entry.Lock.WaitAsync(TimeSpan.FromSeconds(10), ct);

            try
            {
                // Double-check after acquiring the locks
                if (entry.Consumers.TryGetValue(fullBucketAddressId, out existing))
                {
                    return existing;
                }

                // 2. With the caller (none for repository/dispatcher calls), at most 10 s so a boot doesn't hang if NATS
                // is unreachable.
                using var cts = StopControlFactory.ForOperation(ct, TimeSpan.FromSeconds(10));
                var token = cts.Token;

                var consumerName = NatsJetStreamUtils.GetConsumerName(fullBucketAddressId);
                var subject = NatsJetStreamUtils.GetSubjectName(agentConnectionId, fullBucketAddressId);

                bufferSize ??= new WorkerDefinition().BucketBufferSize; // Get default buffer size if not specified.
                var effectiveThreshold = transientThreshold ?? NatsJetStreamConstants.MaxThreshold;
                if (effectiveThreshold > NatsJetStreamConstants.MaxThreshold)
                {
                    effectiveThreshold = NatsJetStreamConstants.MaxThreshold;
                }

                var maxAckPending = NatsJetStreamConstants.CalcMaxAckPending(bufferSize.Value, effectiveThreshold);
                
                bucketBufferLeadTime ??= new WorkerDefinition().BucketBufferLeadTime;
                var ackWait = NatsJetStreamConstants.CalcAckWait(bucketBufferLeadTime.Value);
                
                var consumerConfig = new ConsumerConfig(consumerName)
                {
                    FilterSubject = subject,
                    DurableName = consumerName,
                    AckPolicy = ConsumerConfigAckPolicy.Explicit,
                    DeliverPolicy = ConsumerConfigDeliverPolicy.All,
                    MaxAckPending = maxAckPending, 
                    AckWait = ackWait,
                    MaxDeliver = NatsJetStreamConstants.MaxDeliver,
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
    
    private async ValueTask<INatsJSConsumer> GetOrCreateConsumerInternalAsync(string agentConnectionId, JobMasterAgentProviderConfig config, string fullBucketAddressId)
    {
        // Return cached consumer if present
        var entry = entries[GetEntryKey(agentConnectionId, config)];
        if (entry.Consumers.TryGetValue(fullBucketAddressId, out var existing))
        {
            return existing;
        }

        return await CreateOrUpdateConsumerInternalAsync(agentConnectionId, config, fullBucketAddressId);
    } 
    
    private static string NormalizeNatsUrl(string? url)
    {
        if (string.IsNullOrWhiteSpace(url))
        {
            throw new ArgumentException("NATS connection string is null or empty.", nameof(url));
        }

        var trimmed = url!.Trim();

        // Common when values come from JSON/env and end up with surrounding quotes
        trimmed = trimmed.Trim('"', '\'', ' ', '\t', '\r', '\n');

        var sepIdx = trimmed.IndexOfAny(new[] { ';', '\n', '\r' });
        if (sepIdx > 0)
        {
            trimmed = trimmed[..sepIdx];
        }

        trimmed = trimmed.Trim();

        if (Uri.TryCreate(trimmed, UriKind.Absolute, out var uri) && !string.IsNullOrWhiteSpace(uri.Host))
        {
            return trimmed;
        }

        // Auto-fix a very common configuration pitfall: password contains '@'.
        // In URI syntax, '@' separates userinfo from host, so it must be percent-encoded inside the password.
        // Example:
        //   nats://user:MyP@ss@localhost:4222  ->  nats://user:MyP%40ss@localhost:4222
        try
        {
            if (trimmed.StartsWith("nats://", StringComparison.OrdinalIgnoreCase))
            {
                var afterScheme = trimmed["nats://".Length..];
                var lastAt = afterScheme.LastIndexOf('@');
                var firstAt = afterScheme.IndexOf('@');

                if (firstAt >= 0)
                {
                    var userInfo = afterScheme[..lastAt];
                    var hostPart = afterScheme[(lastAt + 1)..];

                    var colonIdx = userInfo.IndexOf(':');
                    if (colonIdx > 0 && colonIdx < userInfo.Length - 1)
                    {
                        var user = userInfo[..colonIdx];
                        var pass = userInfo[(colonIdx + 1)..];
                        var escapedPass = Uri.EscapeDataString(pass);
                        var rebuilt = $"nats://{user}:{escapedPass}@{hostPart}";

                        if (Uri.TryCreate(rebuilt, UriKind.Absolute, out var uri2) && !string.IsNullOrWhiteSpace(uri2.Host))
                        {
                            return rebuilt;
                        }
                    }
                }
            }
        }
        catch
        {
            // Ignore and throw the final error below
        }

        throw new ArgumentException($"Invalid NATS URL: '{trimmed}'. Original value length: {url.Length}.", nameof(url));
    }


#if NET6_0_OR_GREATER
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
        var keys = entries.Keys
            .Where(k => k == agentConnectionId || k.StartsWith($"{agentConnectionId}::", StringComparison.Ordinal))
            .ToList();

        foreach (var key in keys)
        {
            if (!entries.TryRemove(key, out var e))
                continue;

            try { await e.Nats.DisposeAsync(); } catch { }
            try { e.Lock.Dispose(); } catch { }
        }
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
        var keys = entries.Keys
            .Where(k => k == agentConnectionId || k.StartsWith($"{agentConnectionId}::", StringComparison.Ordinal))
            .ToList();

        foreach (var key in keys)
        {
            if (!entries.TryRemove(key, out var e))
                continue;

            try { AsyncContext.Run(async () => await e.Nats.DisposeAsync()); } catch { }
            try { e.Lock.Dispose(); } catch { }
        }
    }
#endif
}
