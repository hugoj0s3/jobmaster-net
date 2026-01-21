using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Background;

/// <summary>
/// Defines the contract for JobMaster runner components that handle background processing tasks.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public interface IJobMasterRunner
{
    Task OnErrorAsync(Exception ex, CancellationToken ct);
    Task OnStartAsync(CancellationToken ct);
    Task<OnTickResult> OnTickAsync(CancellationToken ct);
    Task OnStopAsync();
    TimeSpan SucceedInterval { get; }
    
    TimeSpan WarmUpInterval { get; }

    int ConsecutiveFailedCount { get; }
    
    Task StartAsync();
    
    Task StopAsync();
    
    IJobMasterRuntime? Runtime { get; }
}