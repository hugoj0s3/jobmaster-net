namespace JobMaster.Sdk.Contracts.Background;

public class OnTickResult
{
    public TicketResultStatus Status { get; }
    public TimeSpan Delay { get; }
    public Exception? Exception { get; }
    public string? ErrorMessage { get; }

    protected OnTickResult(TicketResultStatus status, TimeSpan delay, Exception? exception = null, string? errorMessage = null)
    {
        Status = status;
        Delay = delay;
        Exception = exception;
        ErrorMessage = errorMessage ?? exception?.Message;
    }

    public static OnTickResult Success(TimeSpan delay) 
        => new OnTickResult(TicketResultStatus.Success, delay);

    public static OnTickResult Failed(TimeSpan retryAfter, Exception? exception = null, string? errorMessage = null) 
        => new OnTickResult(TicketResultStatus.Failed, retryAfter, exception, errorMessage);

    public static OnTickResult Locked(TimeSpan retryAfter) 
        => new OnTickResult(TicketResultStatus.Locked, retryAfter);

    public static OnTickResult Skipped(TimeSpan retryAfter) 
        => new OnTickResult(TicketResultStatus.Skipped, retryAfter);

    public static OnTickResult Success(IJobMasterRunner runner) => new OnTickResult(TicketResultStatus.Success, runner.SucceedInterval);
    

    public static OnTickResult Failed(IJobMasterRunner runner, Exception? exception = null, string? errorMessage = null) 
        => new OnTickResult(TicketResultStatus.Failed, new TimeSpan(runner.SucceedInterval.Ticks * 2), exception, errorMessage);

    public static OnTickResult Locked(IJobMasterRunner runner) 
        => new OnTickResult(TicketResultStatus.Locked, new TimeSpan((int)(runner.SucceedInterval.Ticks / 1.5)));

    public static OnTickResult Skipped(IJobMasterRunner runner) 
        => new OnTickResult(TicketResultStatus.Skipped, new TimeSpan((int)(runner.SucceedInterval.Ticks * 1.5)));
}