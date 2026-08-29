namespace JobMaster.Benchmarks.Common.Cli;

/// <summary>Framework-agnostic CLI options shared by any runner that doesn't need JobMaster-specific
/// concepts (buckets/buffer-size/skip-warmup, which have no analog in Quartz/Hangfire's peer-to-peer
/// clustering). <c>JobMaster.Benchmarks.Runner</c>'s own <c>CliOptions</c> stays independent since it
/// has real additional fields.</summary>
public sealed class CliOptions
{
    public required int TargetJobsPerMinute { get; init; }
    public required int WorkerCount { get; init; }
    public required TimeSpan Duration { get; init; }
    public required bool Smoke { get; init; }
    public required TimeSpan DelayMin { get; init; }
    public required TimeSpan DelayMax { get; init; }
    public TimeSpan? GraceOverride { get; init; }
    public string? OutputDirectory { get; init; }

    public int? StepDownJobsPerMinute { get; init; }
    public TimeSpan? StepDownAt { get; init; }
    public int? MaxConcurrentRequests { get; init; }
    public TimeSpan WarmupDelay { get; init; } = TimeSpan.Zero;

    /// <summary>When set, this run is a "burst" capacity test (flood this many immediate jobs with
    /// no rate pacing, then measure drain time) instead of the default paced steady-arrival test.</summary>
    public int? BurstTotalJobs { get; init; }

    /// <summary>Number of parallel requests fired per worker during a burst -- see
    /// <see cref="JobMaster.Benchmarks.Common.Load.LoadGeneratorOptions.BurstRequestsPerWorker"/>.</summary>
    public int BurstRequestsPerWorker { get; init; } = 3;
    public TimeSpan BurstMaxWait { get; init; } = TimeSpan.FromMinutes(60);

    /// <summary>DB container resource limits -- defaults match the original fixed 2 CPU / 2GB spec.
    /// Overridable so burst tiers can scale the DB up (capped at 16GB) alongside worker count.</summary>
    public double DbCpu { get; init; } = 2;
    public double DbMemoryGb { get; init; } = 2;

    /// <summary>Per-worker-container resource limits -- defaults match the original fixed 0.5 CPU /
    /// 512MB spec, mirroring <c>JobMaster.Benchmarks.Runner</c>'s own <c>CliOptions</c>.</summary>
    public double WorkerCpu { get; init; } = 0.5;
    public double WorkerMemoryGb { get; init; } = 0.5;

    public static CliOptions Parse(string[] args)
    {
        var rate = 1000;
        var workers = 3;
        var durationMinutes = 60;
        var smoke = false;
        string? output = null;
        TimeSpan? graceOverride = null;
        int? stepDownRate = null;
        double? stepDownAtMinutes = null;
        int? maxConcurrentRequests = null;
        var warmupDelaySeconds = 0.0;
        int? burstTotalJobs = null;
        var burstRequestsPerWorker = 3;
        var burstMaxWaitMinutes = 60.0;
        var dbCpu = 2.0;
        var dbMemoryGb = 2.0;
        var workerCpu = 0.5;
        var workerMemoryGb = 0.5;

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
                case "--burst-max-wait-minutes":
                    burstMaxWaitMinutes = double.Parse(args[++i]);
                    break;
                case "--db-cpu":
                    dbCpu = double.Parse(args[++i]);
                    break;
                case "--db-memory-gb":
                    dbMemoryGb = double.Parse(args[++i]);
                    break;
                case "--worker-cpu":
                    workerCpu = double.Parse(args[++i]);
                    break;
                case "--worker-memory-gb":
                    workerMemoryGb = double.Parse(args[++i]);
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
                case "--workers":
                    workers = int.Parse(args[++i]);
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

        // Same smoke-mode / step-down-split / delay-max-capping rationale as the JobMaster runner's
        // CliOptions -- see that file's comments for the full reasoning.
        var duration = smoke ? TimeSpan.FromMinutes(3) : TimeSpan.FromMinutes(durationMinutes);
        var delayMin = smoke ? TimeSpan.FromSeconds(30) : TimeSpan.FromMinutes(5);

        var stepDownAt = stepDownAtMinutes.HasValue
            ? TimeSpan.FromMinutes(stepDownAtMinutes.Value)
            : stepDownRate.HasValue ? duration * 0.75 : (TimeSpan?)null;

        var delayMax = smoke
            ? TimeSpan.FromSeconds(90)
            : stepDownAt.HasValue ? duration - stepDownAt.Value : TimeSpan.FromMinutes(30);

        return new CliOptions
        {
            TargetJobsPerMinute = rate,
            WorkerCount = workers,
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
            BurstTotalJobs = burstTotalJobs,
            BurstRequestsPerWorker = burstRequestsPerWorker,
            BurstMaxWait = TimeSpan.FromMinutes(burstMaxWaitMinutes),
            DbCpu = dbCpu,
            DbMemoryGb = dbMemoryGb,
            WorkerCpu = workerCpu,
            WorkerMemoryGb = workerMemoryGb
        };
    }
}
