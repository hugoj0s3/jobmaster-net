using System;
using System.Threading;
using System.Threading.Tasks;
using NATS.Client.JetStream;
using JobMaster.NatsJetStream;

namespace JobMaster.NatsJetStream.Background;

internal sealed class MsgAckGuard : IDisposable
{
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, uint> FailureAttempts = new();
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, int> BusyRetryCount = new();
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, DateTime> LastUpdatedAt = new();
    private static readonly Timer CleanupTimer = new(_ => Cleanup(), null, TimeSpan.FromHours(1), TimeSpan.FromHours(1));

    private readonly SemaphoreSlim semaphore = new(1, 1);
    public INatsJSMsg<byte[]> Msg { get; }
    public AckOutcome Outcome { get; private set; } = AckOutcome.None;
    public uint FailureCount { get; private set; }
    private readonly string messageId;

    public MsgAckGuard(INatsJSMsg<byte[]> msg, string messageId)
    {
        this.Msg = msg;
        this.messageId = messageId;
        this.FailureCount = FailureAttempts.GetOrAdd(messageId, 0);
    }

    public async Task<bool> TryAckSuccessAsync()
    {
        await semaphore.WaitAsync();
        try
        {
            if (Outcome != AckOutcome.None) return false;

            using var cts = new CancellationTokenSource(NatsJetStreamConstants.AckOperationTimeout);
            await Msg.AckAsync(cancellationToken: cts.Token);
            Outcome = AckOutcome.Ack;
            FailureAttempts.TryRemove(messageId, out _);
            BusyRetryCount.TryRemove(messageId, out _);
            LastUpdatedAt.TryRemove(messageId, out _);

            return true;
        }
        finally { semaphore.Release(); }
    }

    public async Task<bool> TryNakAsync(TimeSpan delay)
    {
        await semaphore.WaitAsync();
        try
        {
            if (Outcome != AckOutcome.None) return false;

            using var cts = new CancellationTokenSource(NatsJetStreamConstants.AckOperationTimeout);
            await Msg.NakAsync(delay: delay, cancellationToken: cts.Token);
            Outcome = AckOutcome.Nak;

            return true;
        }
        finally { semaphore.Release(); }
    }

    public async Task<bool> TryNakFailAsync()
    {
        await semaphore.WaitAsync();
        try
        {
            if (Outcome != AckOutcome.None) return false;

            FailureCount = FailureAttempts.AddOrUpdate(messageId, 1, (_, count) => count + 1);
            LastUpdatedAt[messageId] = DateTime.UtcNow;

            // Exponential backoff based on failure count: 1s, 5s, 15s, 30s, 60s...
            var delaySeconds = FailureCount switch
            {
                1 => 1,
                2 => 5,
                3 => 15,
                4 => 30,
                _ => 60
            };
            var delay = TimeSpan.FromSeconds(delaySeconds);

            using var cts = new CancellationTokenSource(NatsJetStreamConstants.AckOperationTimeout);
            await Msg.NakAsync(delay: delay, cancellationToken: cts.Token);
            Outcome = AckOutcome.Nak;

            return true;
        }
        finally { semaphore.Release(); }
    }

    public async Task TryAckProgressAsync()
    {
        await semaphore.WaitAsync();
        try
        {
            if (Outcome != AckOutcome.None) return;

            using var cts = new CancellationTokenSource(NatsJetStreamConstants.AckOperationTimeout);
            await Msg.AckProgressAsync(cancellationToken: cts.Token);
        }
        finally { semaphore.Release(); }
    }

    public async Task<bool> TryAckTerminateAsync()
    {
        await semaphore.WaitAsync();
        try
        {
            if (Outcome != AckOutcome.None) return false;

            using var cts = new CancellationTokenSource(NatsJetStreamConstants.AckOperationTimeout);
            await Msg.AckTerminateAsync(cancellationToken: cts.Token);
            Outcome = AckOutcome.Term;

            return true;
        }
        finally { semaphore.Release(); }
    }

    public async Task<NackBusyResult> TryNakBusyAsync()
    {
        await semaphore.WaitAsync();
        try
        {
            if (Outcome != AckOutcome.None) return NackBusyResult.Fail;

            BusyRetryCount.TryGetValue(messageId, out var retryCount);
            retryCount++;

            if (retryCount >= NatsJetStreamConstants.BusyRetryDelays.Length)
            {
                BusyRetryCount.TryRemove(messageId, out _);
                LastUpdatedAt.TryRemove(messageId, out _);
                return NackBusyResult.RetriesExhausted;
            }

            BusyRetryCount[messageId] = retryCount;
            LastUpdatedAt[messageId] = DateTime.UtcNow;

            var delay = NatsJetStreamConstants.BusyRetryDelays[retryCount - 1];
            using var cts = new CancellationTokenSource(NatsJetStreamConstants.AckOperationTimeout);
            await Msg.NakAsync(delay: delay, cancellationToken: cts.Token);
            Outcome = AckOutcome.Nak;

            return NackBusyResult.Retry;
        }
        finally { semaphore.Release(); }
    }

    private static void Cleanup()
    {
        var cutoff = DateTime.UtcNow.AddHours(-2);
        foreach (var kvp in LastUpdatedAt)
        {
            if (kvp.Value < cutoff)
            {
                LastUpdatedAt.TryRemove(kvp.Key, out _);
                FailureAttempts.TryRemove(kvp.Key, out _);
                BusyRetryCount.TryRemove(kvp.Key, out _);
            }
        }
    }

    public void Dispose() => semaphore.Dispose();
}

internal enum NackBusyResult
{
    Retry,
    RetriesExhausted,
    Fail
}
