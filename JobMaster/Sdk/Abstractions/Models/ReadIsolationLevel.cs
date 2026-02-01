namespace JobMaster.Sdk.Abstractions.Models;

internal enum ReadIsolationLevel
{
    /// <summary>
    /// Strict consistency. Prevents "dirty reads."
    /// Use for critical state changes where accuracy is more important than speed.
    /// (Maps to: Read Committed on SQL.)
    /// </summary>
    Consistent = 1,

    /// <summary>
    /// High-performance, non-blocking read. Allows "dirty reads."
    /// Ideal for Logs, Metrics, and Heartbeats where you must not lock the tables.
    /// (Maps to: Read Uncommitted on SQL.)
    /// </summary>
    FastSync = 2,
}