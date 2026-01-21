using System;
using System.Threading;
using System.Threading.Tasks;
using NATS.Client.JetStream;

namespace JobMaster.NatJetStream.Background;

internal sealed class MsgAckGuard
{
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, uint> FailureAttempts = new();
    
    public INatsJSMsg<byte[]> Msg { get; }
    public AckOutcome Outcome { get; private set; } = AckOutcome.None;
    public uint FailureCount { get; private set; }

    public MsgAckGuard(INatsJSMsg<byte[]> msg, string messageId)
    {
        this.Msg = msg;
        this.FailureCount = FailureAttempts.GetOrAdd(messageId, 0);
    }
    

    public async Task<bool> TryAckSuccessAsync(string messageId)
    {
        if (Outcome != AckOutcome.None) return false;
        
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await Msg.AckAsync(cancellationToken: cts.Token);
        Outcome = AckOutcome.Ack;
        ClearFailure(messageId);
        
        return true;
    }

    public async Task<bool> TryNakAsync(TimeSpan delay)
    {
        if (Outcome != AckOutcome.None) return false;
        
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await Msg.NakAsync(delay: delay, cancellationToken: cts.Token);
        Outcome = AckOutcome.Nak;
        
        return true;
    }
    
    public async Task<bool> TryNakFailAsync(string messageId)
    {
        if (Outcome != AckOutcome.None) return false;
        
        IncrementFailure(messageId);
        
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
        
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await Msg.NakAsync(delay: delay, cancellationToken: cts.Token);
        Outcome = AckOutcome.Nak;
        
        return true;
    }

    public async Task<bool> TryAckTerminateAsync()
    {
        if (Outcome != AckOutcome.None) return false;
        
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await Msg.AckTerminateAsync(cancellationToken: cts.Token);
        Outcome = AckOutcome.Term;
        
        return true;
    }
    
    private void IncrementFailure(string messageId)
    {
        FailureCount = FailureAttempts.AddOrUpdate(messageId, 1, (_, count) => count + 1);
    }
    
    private void ClearFailure(string messageId)
    {
        FailureAttempts.TryRemove(messageId, out _);
    }
}
