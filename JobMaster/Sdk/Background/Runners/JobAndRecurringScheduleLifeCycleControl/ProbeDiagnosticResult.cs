namespace JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;

/// <summary>
/// Outcome of <c>ProbeDiagnosticAsync</c>: whether this worker should skip the tick,
/// wait (another worker won the lock), or proceed with job assignment.
/// </summary>
internal enum ProbeDiagnosticAction
{
    /// <summary>No imminent jobs and scan-plan interval has not elapsed — do nothing this tick.</summary>
    Skip,
    /// <summary>Jobs are ready but another worker acquired the coordination lock — skip without retry.</summary>
    Lock,
    /// <summary>This worker acquired the lock and should proceed to acquire and assign jobs.</summary>
    Assign
}

internal class ProbeDiagnosticResult
{
    public ProbeDiagnosticAction Action { get; set; }
    public int BatchSize { get; set; }
    public string? LockKey { get; set; }
    public string? LockToken { get; set; }

    public static ProbeDiagnosticResult Skip() => new() { Action = ProbeDiagnosticAction.Skip };
    public static ProbeDiagnosticResult Locked() => new() { Action = ProbeDiagnosticAction.Lock };
    public bool IsImminent { get; set; }

    public static ProbeDiagnosticResult AssignImminent(int batchSize, string lockKey, string lockToken) =>
        new() { Action = ProbeDiagnosticAction.Assign, IsImminent = true, BatchSize = batchSize, LockKey = lockKey, LockToken = lockToken };

    public static ProbeDiagnosticResult Assign(int batchSize, string lockKey, string lockToken) =>
        new() { Action = ProbeDiagnosticAction.Assign, IsImminent = false, BatchSize = batchSize, LockKey = lockKey, LockToken = lockToken };
}
