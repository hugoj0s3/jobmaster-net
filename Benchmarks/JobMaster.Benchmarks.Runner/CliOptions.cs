using JobMaster.Benchmarks.Common.Containers;

namespace JobMaster.Benchmarks.Runner;

public sealed class CliOptions
{
    public required int TargetJobsPerMinute { get; init; }

    /// <summary>Number of dedicated Execution-only containers. Does not include the drainer or any
    /// coordinator container -- see <see cref="CoordinatorContainerCount"/>.</summary>
    public required int ExecutorCount { get; init; }
    public required int BucketsPerWorker { get; init; }
    public int? BucketBufferSize { get; init; }

    /// <summary>Overrides every executor's ParallelismFactor (SDK default 1.0) -- multiplies the base
    /// per-priority concurrent-execution-slot count for each bucket (Medium=4 base). Null leaves the
    /// SDK default in place.</summary>
    public double? ParallelismFactor { get; init; }

    /// <summary>Total Coordinator instances, split as evenly as possible across
    /// <see cref="CoordinatorContainerCount"/> dedicated containers.</summary>
    public required int CoordinatorCount { get; init; }

    /// <summary>Number of dedicated coordinator containers <see cref="CoordinatorCount"/> instances
    /// are split across. The drainer always gets its own separate container regardless of this
    /// value.</summary>
    public required int CoordinatorContainerCount { get; init; }

    /// <summary>Overrides <c>JobMasterDefaults.Worker.TransferBatchSize</c> (1000) on every coordinator
    /// instance. Null leaves the SDK default in place.</summary>
    public int? TransferBatchSize { get; init; }

    /// <summary>When true, every coordinator container and the drainer mirror every log entry
    /// (including Debug-level tick-timing logs, which are never persisted to the DB) to a JSONL file, copied out
    /// into container-logs/ after the run.</summary>
    public required bool EnableDebugJsonl { get; init; }
    public required bool SkipWarmUpTime { get; init; }
    public required TimeSpan Duration { get; init; }
    public required bool Smoke { get; init; }
    public required TimeSpan DelayMin { get; init; }
    public required TimeSpan DelayMax { get; init; }
    public TimeSpan? GraceOverride { get; init; }
    public string? OutputDirectory { get; init; }

    /// <summary>Which SQL engine backs the master DB and (unless <see cref="UseNats"/>) every agent
    /// connection -- defaults to Postgres, preserving this runner's original PostgresPure-only
    /// behavior.</summary>
    public required DbEngine DbEngine { get; init; }

    /// <summary>When true, every agent connection uses NatsJetStream instead of <see cref="DbEngine"/>
    /// -- the master DB stays on <see cref="DbEngine"/> either way (see JobMasterTopologyBuilder).</summary>
    public required bool UseNats { get; init; }

    /// <summary>When true, every drain/execution worker shares one agent connection/database instead
    /// of each getting its own dedicated one -- isolates "how many databases exist" as its own
    /// benchmark variable, separate from bucket count.</summary>
    public required bool SharedAgentConnection { get; init; }

    /// <summary>Optional spike simulation: start at <see cref="TargetJobsPerMinute"/> and step down
    /// to this rate once <see cref="StepDownAt"/> elapses, to confirm an accumulated backlog
    /// actually drains once demand drops below capacity.</summary>
    public int? StepDownJobsPerMinute { get; init; }
    public TimeSpan? StepDownAt { get; init; }

    /// <summary>Overrides the load generator's cap on in-flight concurrent schedule requests --
    /// diagnostic knob to isolate whether a failure mode is triggered by concurrent scheduling
    /// requests specifically.</summary>
    public int? MaxConcurrentRequests { get; init; }

    /// <summary>Delay between containers reporting ready (HTTP health check passing) and load
    /// generation starting -- diagnostic knob to test whether early requests race a worker's
    /// background bucket-creation step before it completes.</summary>
    public TimeSpan WarmupDelay { get; init; } = TimeSpan.Zero;

    /// <summary>When set, this run is a "burst" capacity test (flood this many immediate jobs with
    /// no rate pacing, then measure drain time) instead of the default paced steady-arrival test --
    /// see <see cref="Common.Load.LoadGeneratorOptions.BurstTotalJobs"/>.</summary>
    public int? BurstTotalJobs { get; init; }

    /// <summary>Number of parallel requests fired per worker during a burst -- see
    /// <see cref="Common.Load.LoadGeneratorOptions.BurstRequestsPerWorker"/>.</summary>
    public int BurstRequestsPerWorker { get; init; } = 3;

    /// <summary>When set alongside <see cref="BurstTotalJobs"/>, every burst job is scheduled with
    /// this fixed delay instead of immediately -- fires a burst of jobs that all become due at
    /// roughly the same time, isolating "many jobs claimable/completing at once" from general
    /// backlog size or concurrency. See <see cref="Common.Load.LoadGeneratorOptions.BurstDelay"/>.</summary>
    public TimeSpan? BurstDelay { get; init; }

    /// <summary>Max time to wait for a burst to fully drain before giving up and reporting whatever
    /// didn't finish as lost -- burst runs have no DelayMax to derive a grace period from, unlike the
    /// paced test, so this is an explicit safety cap instead.</summary>
    public TimeSpan BurstMaxWait { get; init; } = TimeSpan.FromMinutes(60);

    /// <summary>DB container resource limits -- defaults match the original fixed 2 CPU / 2GB spec.
    /// Overridable so burst tiers can scale the DB up (capped at 16GB) alongside worker count.</summary>
    public double DbCpu { get; init; } = 2;
    public double DbMemoryGb { get; init; } = 2;

    public static CliOptions Parse(string[] args)
    {
        var rate = 1000;
        var executors = 2;
        var coordinatorContainerCount = 1;
        double? parallelismFactor = null;
        var bucketsPerWorker = 1;
        int? bucketBufferSize = null;
        var skipWarmUpTime = false;
        var durationMinutes = 60;
        var smoke = false;
        string? output = null;
        TimeSpan? graceOverride = null;
        int? stepDownRate = null;
        double? stepDownAtMinutes = null;
        int? maxConcurrentRequests = null;
        var warmupDelaySeconds = 0.0;
        var dbEngine = DbEngine.Postgres;
        var useNats = false;
        var sharedAgentConnection = false;
        int? burstTotalJobs = null;
        var burstRequestsPerWorker = 3;
        double? burstDelayMinutes = null;
        var burstMaxWaitMinutes = 60.0;
        var dbCpu = 2.0;
        var dbMemoryGb = 2.0;
        var coordinatorCount = 4;
        int? transferBatchSize = null;
        var enableDebugJsonl = false;

        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--burst-total":
                    burstTotalJobs = int.Parse(args[++i]);
                    break;
                case "--burst-requests-per-worker":
                    burstRequestsPerWorker = int.Parse(args[++i]);
                    break;
                case "--burst-delay-minutes":
                    burstDelayMinutes = double.Parse(args[++i]);
                    break;
                case "--burst-max-wait-minutes":
                    burstMaxWaitMinutes = double.Parse(args[++i]);
                    break;
                case "--db-cpu":
                    dbCpu = double.Parse(args[++i]);
                    break;
                case "--db-memory-gb":
                    dbMemoryGb = double.Parse(args[++i]);
                    break;
                case "--coordinators":
                    coordinatorCount = int.Parse(args[++i]);
                    break;
                case "--coordinator-containers":
                    coordinatorContainerCount = int.Parse(args[++i]);
                    break;
                case "--parallelism-factor":
                    parallelismFactor = double.Parse(args[++i]);
                    break;
                case "--transfer-batch-size":
                    transferBatchSize = int.Parse(args[++i]);
                    break;
                case "--debug-jsonl":
                    enableDebugJsonl = true;
                    break;
                case "--db":
                    dbEngine = args[++i].ToLowerInvariant() switch
                    {
                        "postgres" => DbEngine.Postgres,
                        "mysql" => DbEngine.MySql,
                        "sqlserver" => DbEngine.SqlServer,
                        "ravendb" => DbEngine.RavenDB,
                        var other => throw new ArgumentException($"Unknown --db value '{other}'. Expected postgres, mysql, sqlserver, or ravendb."),
                    };
                    break;
                case "--nats":
                    useNats = true;
                    break;
                case "--shared-agent":
                    sharedAgentConnection = true;
                    break;
                case "--rate":
                    rate = int.Parse(args[++i]);
                    break;
                case "--step-down-rate":
                    stepDownRate = int.Parse(args[++i]);
                    break;
                case "--step-down-at-minutes":
                    stepDownAtMinutes = double.Parse(args[++i]);
                    break;
                case "--executors":
                    executors = int.Parse(args[++i]);
                    break;
                case "--buckets":
                    bucketsPerWorker = int.Parse(args[++i]);
                    break;
                case "--buffer-size":
                    bucketBufferSize = int.Parse(args[++i]);
                    break;
                case "--skip-warmup":
                    skipWarmUpTime = true;
                    break;
                case "--duration-minutes":
                    durationMinutes = int.Parse(args[++i]);
                    break;
                case "--smoke":
                    smoke = true;
                    break;
                case "--grace-minutes":
                    graceOverride = TimeSpan.FromMinutes(double.Parse(args[++i]));
                    break;
                case "--output":
                    output = args[++i];
                    break;
                case "--max-concurrent-requests":
                    maxConcurrentRequests = int.Parse(args[++i]);
                    break;
                case "--warmup-delay-seconds":
                    warmupDelaySeconds = double.Parse(args[++i]);
                    break;
                default:
                    throw new ArgumentException($"Unknown argument '{args[i]}'.");
            }
        }

        // Smoke mode: a short run (a few minutes) to validate the whole pipeline -- containers,
        // load generator, latency join, stats CSVs, report -- before committing to a full hour.
        // The delayed-job range shrinks to match -- the default 5-30 minute delay would make even a
        // 3-minute smoke run take 30+ minutes just waiting for the grace period.
        var duration = smoke ? TimeSpan.FromMinutes(3) : TimeSpan.FromMinutes(durationMinutes);
        var delayMin = smoke ? TimeSpan.FromSeconds(30) : TimeSpan.FromMinutes(5);

        // Default split for a spike/step-down run: the peak ("pick") phase runs for the first 3/4
        // of the duration, the step-down phase for the last 1/4 -- overridable via
        // --step-down-at-minutes for a custom split.
        var stepDownAt = stepDownAtMinutes.HasValue
            ? TimeSpan.FromMinutes(stepDownAtMinutes.Value)
            : stepDownRate.HasValue ? duration * 0.75 : (TimeSpan?)null;

        // When a step-down is configured, cap the delayed-job range at exactly the time remaining
        // after the step-down point -- so the last delayed job scheduled during the pick phase
        // becomes due right at the end of the run, and every job's due time has already passed by
        // the time the load generator itself finishes (only draining/execution time is then left to
        // wait out during the grace period, not more delays still ticking down).
        var delayMax = smoke
            ? TimeSpan.FromSeconds(90)
            : stepDownAt.HasValue ? duration - stepDownAt.Value : TimeSpan.FromMinutes(30);

        return new CliOptions
        {
            TargetJobsPerMinute = rate,
            ExecutorCount = executors,
            BucketsPerWorker = bucketsPerWorker,
            BucketBufferSize = bucketBufferSize,
            CoordinatorCount = coordinatorCount,
            CoordinatorContainerCount = coordinatorContainerCount,
            ParallelismFactor = parallelismFactor,
            TransferBatchSize = transferBatchSize,
            EnableDebugJsonl = enableDebugJsonl,
            SkipWarmUpTime = skipWarmUpTime,
            Duration = duration,
            Smoke = smoke,
            DelayMin = delayMin,
            DelayMax = delayMax,
            GraceOverride = graceOverride,
            OutputDirectory = output,
            StepDownJobsPerMinute = stepDownRate,
            StepDownAt = stepDownAt,
            MaxConcurrentRequests = maxConcurrentRequests,
            WarmupDelay = TimeSpan.FromSeconds(warmupDelaySeconds),
            DbEngine = dbEngine,
            UseNats = useNats,
            SharedAgentConnection = sharedAgentConnection,
            BurstTotalJobs = burstTotalJobs,
            BurstRequestsPerWorker = burstRequestsPerWorker,
            BurstDelay = burstDelayMinutes.HasValue ? TimeSpan.FromMinutes(burstDelayMinutes.Value) : null,
            BurstMaxWait = TimeSpan.FromMinutes(burstMaxWaitMinutes),
            DbCpu = dbCpu,
            DbMemoryGb = dbMemoryGb
        };
    }
}
