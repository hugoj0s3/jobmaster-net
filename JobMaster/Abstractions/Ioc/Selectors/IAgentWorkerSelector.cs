using JobMaster.Abstractions.Models;

namespace JobMaster.Abstractions.Ioc.Selectors;

/// <summary>
/// Fluent configuration for a single worker within an agent.
/// A worker is the unit that pulls jobs from the master scheduler and executes them.
/// Use this selector to control which jobs the worker handles, how many it pulls at once,
/// its parallelism, and its operational mode.
/// All methods return the same selector instance to allow method chaining.
/// </summary>
public interface IAgentWorkerSelector
{
    /// <summary>
    /// Binds this worker to the named agent connection.
    /// Required when more than one agent connection is registered on the cluster.
    /// </summary>
    /// <param name="agentConnName">The logical name of the agent connection this worker belongs to.</param>
    IAgentWorkerSelector AgentConnName(string agentConnName);

    /// <summary>
    /// Sets a display name for this worker instance.
    /// Useful for identifying the worker in logs and dashboards when multiple workers are running.
    /// If not specified, the name is auto-generated as <c>{hostname}-{timestampId}</c>
    /// (e.g. <c>myserver-3c1a8b2</c>). When specified, the same timestamp suffix is appended
    /// to guarantee uniqueness across restarts (e.g. <c>payroll-01-3c1a8b2</c>).
    /// </summary>
    /// <param name="workerName">A human-readable name for this worker (letters, numbers, hyphens, underscores; max 25 chars).</param>
    IAgentWorkerSelector WorkerName(string workerName);

    /// <summary>
    /// Assigns this worker to a specific lane.
    /// A lane is a logical grouping (e.g. "email", "reports") that lets you dedicate
    /// workers to particular job categories.
    /// Default: null — the worker picks up jobs that have no lane specified.
    /// </summary>
    /// <param name="workerLane">The lane name this worker is dedicated to.</param>
    IAgentWorkerSelector WorkerLane(string workerLane);

    /// <summary>
    /// Controls how many jobs are pulled from the master in a single transfer cycle.
    /// Higher values reduce round-trip overhead but increase memory usage per cycle.
    /// Default: <see cref="JobMasterDefaults.Worker.TransferBatchSize"/> (1000).
    /// </summary>
    /// <param name="batchSize">Number of jobs per transfer batch.</param>
    IAgentWorkerSelector TransferBatchSize(int batchSize);

    /// <summary>
    /// Sets the maximum number of job buckets held in the worker's local in-memory buffer.
    /// A larger buffer keeps the worker busier during bursts but uses more memory.
    /// Default: <see cref="JobMasterDefaults.Worker.BucketBufferSize"/> (250 buckets).
    /// </summary>
    /// <param name="bufferSize">Maximum number of buckets to buffer locally.</param>
    IAgentWorkerSelector BucketBufferSize(int bufferSize);

    /// <summary>
    /// Sets how far ahead the worker pre-fetches job buckets relative to the current time.
    /// A longer lead time smooths execution by ensuring buckets are ready before they are due,
    /// at the cost of holding more jobs in memory.
    /// Default: <see cref="JobMasterDefaults.Worker.BucketBufferLeadTime"/> (15 seconds).
    /// Allowed range: 250 milliseconds (minimum) to 30 seconds (maximum).
    /// </summary>
    /// <param name="bucketDuration">The look-ahead window for pre-fetching buckets.</param>
    IAgentWorkerSelector BucketBufferLeadTime(TimeSpan bucketDuration);

    /// <summary>
    /// Sets how many execution partitions this worker maintains for the given priority level.
    /// Each bucket is exclusively owned by this worker and independently processes jobs of that priority,
    /// enabling horizontal scaling without contention between workers.
    /// A higher count increases concurrent throughput for that priority at the cost of more resource usage.
    /// Setting qty to 0 disables that priority on this worker entirely — useful for dedicated workers
    /// (e.g. a worker that only handles Critical jobs). When doing so, ensure other workers cover the
    /// remaining priorities to avoid those jobs never being processed.
    /// Call this method once per priority level you want to tune.
    /// Default: <see cref="JobMasterDefaults.Worker.BucketQtyPerPriority"/> (1 per priority level).
    /// </summary>
    /// <param name="priority">The job priority level (VeryLow, Low, Medium, High, or Critical).</param>
    /// <param name="qty">Number of independent execution buckets for this priority. Use 0 to disable this priority on the worker.</param>
    IAgentWorkerSelector BucketQtyConfig(JobMasterPriority priority, int qty);

    /// <summary>
    /// Sets the operational role of this worker within the cluster.
    /// Separating roles allows high-scale deployments to isolate Master DB traffic from pure execution load.
    /// Default: <see cref="JobMasterDefaults.Worker.DefaultMode"/> (<see cref="AgentWorkerMode.Full"/>).
    /// See <see cref="AgentWorkerMode"/> for a description of each mode.
    /// </summary>
    /// <param name="mode">The desired worker mode.</param>
    IAgentWorkerSelector SetWorkerMode(AgentWorkerMode mode);

    /// <summary>
    /// Disables the initial warm-up delay so the worker begins pulling jobs immediately after startup.
    /// By default a short warm-up period is applied to let the rest of the application initialize first.
    /// </summary>
    IAgentWorkerSelector SkipWarmUpTime();

    /// <summary>
    /// Scales the concurrent execution capacity of this worker across all priority levels.
    /// The system calculates a run capacity per bucket using:
    /// <c>RunCapacity = BasePriorityCapacity × Factor</c>
    /// where base capacities are: VeryLow=2, Low=3, Medium=4, High=5, Critical=6.
    /// A built-in backpressure mechanism automatically pauses job fetching when the internal
    /// waiting queue reaches <c>RunCapacity × 5</c>, preventing memory exhaustion under heavy load.
    /// Default: <see cref="JobMasterDefaults.Worker.ParallelismFactor"/> (1.0).
    /// </summary>
    /// <param name="factor">
    /// A positive multiplier applied to each priority's base capacity.
    /// Use 1.0 for the default; 2.0 to double throughput; 0.5 to halve it.
    /// </param>
    IAgentWorkerSelector ParallelismFactor(double factor);
}
