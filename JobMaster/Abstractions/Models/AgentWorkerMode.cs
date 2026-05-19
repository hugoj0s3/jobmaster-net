namespace JobMaster.Abstractions.Models;

public enum AgentWorkerMode
{
    /// <summary>
    /// All-in-one mode (recommended for most deployments). Scans the Master DB, onboards jobs into
    /// buckets, executes handlers, and drains orphaned buckets. Start here if unsure.
    /// </summary>
    Full = 1,

    /// <summary>
    /// Executes handlers only. Owns its own buckets and communicates solely with the assigned Agent
    /// connection for job dispatch. Does not scan the Master DB for scheduling — only writes
    /// status updates and enforces execution deadlines, enabling near-infinite horizontal
    /// scaling without adding coordination load to the Master.
    /// </summary>
    Execution = 2,

    /// <summary>
    /// Graceful shutdown mode. Recovers orphaned buckets from crashed workers by redirecting
    /// their jobs back to the Master for re-assignment. Safe to terminate once draining completes.
    /// </summary>
    Drain = 3,

    /// <summary>
    /// Scans the Master DB and onboards jobs into buckets but does not execute handlers.
    /// Assigns orphaned buckets to workers to drain them.
    /// In high-scale systems, deploy a small number of dedicated Coordinators alongside many
    /// Execution workers to keep the pipeline full without adding execution load to the nodes
    /// that talk to the Master.
    /// </summary>
    Coordinator = 4,
}
