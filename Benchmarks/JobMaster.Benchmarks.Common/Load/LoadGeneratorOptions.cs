namespace JobMaster.Benchmarks.Common.Load;

public sealed class LoadGeneratorOptions
{
    public required int TargetJobsPerMinute { get; init; }
    public required TimeSpan Duration { get; init; }

    /// <summary>Optional step-down: once <see cref="StepDownAt"/> elapses from the start of the
    /// run, the target rate switches from <see cref="TargetJobsPerMinute"/> to this value for the
    /// remainder of the run -- simulates a traffic spike that then subsides, to see whether an
    /// accumulated backlog actually drains once demand drops below capacity (rather than being
    /// stuck for some other reason).</summary>
    public int? StepDownJobsPerMinute { get; init; }
    public TimeSpan? StepDownAt { get; init; }

    /// <summary>Fraction of jobs scheduled for immediate execution vs. delayed. Not specified by
    /// Hugo's original spec -- defaulted to 50/50, flagged as an assumption to confirm once Phase A
    /// produces its first real report.</summary>
    public double ImmediateFraction { get; init; } = 0.5;

    /// <summary>Delayed jobs get a uniformly random delay in [DelayMin, DelayMax). Hugo specified
    /// "more than 5 minutes" with no upper bound -- 30 minutes is a placeholder default, same
    /// assumption-flag as ImmediateFraction.</summary>
    public TimeSpan DelayMin { get; init; } = TimeSpan.FromMinutes(5);
    public TimeSpan DelayMax { get; init; } = TimeSpan.FromMinutes(30);

    /// <summary>Target number of jobs per batch tick -- the tick interval is derived from this and
    /// the target rate (see <see cref="LoadGenerator"/>), not fixed, so higher tiers get finer,
    /// more frequent ticks instead of one giant burst every few seconds.</summary>
    public int TargetJobsPerTick { get; init; } = 50;

    public TimeSpan MinTickInterval { get; init; } = TimeSpan.FromMilliseconds(100);
    public TimeSpan MaxTickInterval { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Caps in-flight concurrent schedule requests -- jobs within a tick fire concurrently
    /// (bounded by this), not sequentially, to simulate real concurrent client load rather than an
    /// artificially serialized loop.</summary>
    public int MaxConcurrentRequests { get; init; } = 200;

    /// <summary>Jobs requested per HTTP call. Defaults to 1 (one schedule call per job) to most
    /// closely simulate independent concurrent client requests, per Hugo's "simulate real scenario"
    /// instruction -- raise this to trade realism for less HTTP overhead if a tier's enqueue side
    /// becomes the bottleneck instead of the framework under test.</summary>
    public int JobsPerRequest { get; init; } = 1;

    /// <summary>When set, switches the whole run from the paced steady-arrival model above to a
    /// "burst" capacity test: fire this many immediate jobs in batches of <see cref="BurstBatchSize"/>
    /// with no rate pacing at all (as fast as <see cref="MaxConcurrentRequests"/> allows), then the
    /// caller measures how long it takes every job to actually finish. Simulates a direct flood of
    /// load rather than a steady real-world arrival rate -- answers "what's the max throughput this
    /// framework can drain," which the paced test above never stresses since it always keeps up with
    /// its (modest) configured rate.</summary>
    public int? BurstTotalJobs { get; init; }

    public int BurstBatchSize { get; init; } = 100;

    /// <summary>When set alongside <see cref="BurstTotalJobs"/>, every burst job is scheduled with
    /// this fixed delay instead of immediately -- fires a burst of jobs that all become due at
    /// roughly the same time, rather than a flood of already-due jobs. Used to isolate whether a
    /// slowdown comes from many jobs becoming claimable/completing at once versus general backlog
    /// size or concurrency.</summary>
    public TimeSpan? BurstDelay { get; init; }
}
