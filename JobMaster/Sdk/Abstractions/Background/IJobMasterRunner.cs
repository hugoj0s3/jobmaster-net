namespace JobMaster.Sdk.Abstractions.Background;

internal interface IJobMasterRunner
{
    Task OnTickFailureAsync(Exception ex, CancellationToken ct);
    Task OnTerminateFailureAsync(Exception lastException);
    Task OnStartAsync(CancellationToken ct);
    Task<OnTickResult> OnTickAsync(CancellationToken ct);
    Task OnStopAsync();
    TimeSpan SucceedInterval { get; }
    
    TimeSpan WarmUpInterval { get; }
    
    TimeSpan FailedInterval { get; }
    
    double ConsecutiveFailedCount { get; }
    
    Task StartAsync();
    
    Task StopAsync();
    
    IJobMasterRuntime? Runtime { get; }
}