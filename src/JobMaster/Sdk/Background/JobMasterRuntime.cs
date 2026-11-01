using System.Collections.Concurrent;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Reflection;
using JobMaster.Sdk.Utils;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Sdk.Background;

internal class JobMasterRuntime : IJobMasterRuntime
{
    private readonly List<JobMasterBackgroundAgentWorker> Workers = new List<JobMasterBackgroundAgentWorker>();

    // Values one StartAsync run computes and reads later in the same run; cleared when it ends.
    private readonly StartupState State = new StartupState();

    public bool Started { get; private set; } = false;
    public DateTime? StartedAt { get; private set; }

    public DateTime? StartingAt { get; private set; }

    public IReadOnlyList<IJobMasterBackgroundAgentWorker> GetAllWorkers() =>
        new List<IJobMasterBackgroundAgentWorker>(Workers);

    public bool IsOnWarmUpTime()
    {
        return Started &&
               StartedAt.HasValue &&
               (DateTime.UtcNow - StartedAt.Value) <= TimeSpan.FromMinutes(2.5);
    }

    public JobMasterRuntime()
    {
    }

    public async Task StartAsync(IServiceProvider serviceProvider)
    {
        try
        {
            await DoStartAsync(serviceProvider);
        }
        finally
        {
            // Also after a failed start, so a retry doesn't see its values.
            State.Clear();
        }
    }

    private async Task DoStartAsync(IServiceProvider serviceProvider)
    {
        this.StartingAt = DateTime.UtcNow;
        if (serviceProvider == null) throw new ArgumentNullException(nameof(serviceProvider));

        if (Started)
        {
            throw new InvalidOperationException("JobMasterRuntime is already started");
        }

        using var scope = serviceProvider.CreateScope();

        ValidateClusterDefinitions(State.ValidationMessages);

        // All validation must complete before anything starts provisioning/side-effecting, and is reported as one list.
        var runtimeSetups = scope.ServiceProvider.GetServices<IJobMasterRuntimeSetup>().ToList();
        foreach (var runtimeSetup in runtimeSetups)
        {
            State.ValidationMessages.AddRange(await runtimeSetup.ValidateAsync(scope.ServiceProvider));
        }

        // Built now so a broken profile or definition fails here; saved after the workers start. No default means its
        // error is already in the list.
        var defaultClusterId = JobMasterClusterConnectionConfig.Default?.ClusterId;
        if (defaultClusterId != null)
        {
            var staticSchedules = StaticRecurringScheduleDefinitionsBuilder.Build(defaultClusterId, BootstrapBlueprintDefinitions.Clusters);
            State.ValidationMessages.AddRange(staticSchedules.Errors);
            State.StaticSchedules = staticSchedules.Definitions;
        }

        ThrowIfFailed(State.ValidationMessages);

        // Read-only, so before provisioning: nothing is created until every other check passed. Only once the
        // configuration is valid, so a misconfigured cluster is never queried.
        await ValidateArchivedStorageAsync();
        ThrowIfFailed(State.ValidationMessages);

        foreach (var runtimeSetup in runtimeSetups)
        {
            await runtimeSetup.OnBeforeStartAsync(scope.ServiceProvider);
        }

        // After provisioning: it writes each agent's own fingerprint (get-or-create) and needs its tables. Still before
        // any save.
        await ValidateFingerprintAsync();
        ThrowIfFailed(State.ValidationMessages);

        foreach (var clusterCnnCfg in JobMasterClusterConnectionConfig.GetAllConfigs())
        {
            var componentFactory = JobMasterClusterAwareComponentFactories.GetFactory(clusterCnnCfg.ClusterId);
            var agentComponentFactory = componentFactory.GetComponent<IAgentComponentFactory>();
            var masterConfigService = componentFactory.GetComponent<IMasterClusterConfigurationService>();
            var masterAgentConnectionService = componentFactory.GetComponent<IMasterAgentConnectionService>();
            var logger = componentFactory.GetComponent<IJobMasterLogger>();

            var clusterDefinition = BootstrapBlueprintDefinitions.Clusters.Single(c =>
                string.Equals(c.ClusterId, clusterCnnCfg.ClusterId, StringComparison.OrdinalIgnoreCase));

            var modelToSave = masterConfigService.GetFresh() ?? new ClusterConfigurationModel(clusterCnnCfg.ClusterId);
            var isStandalone = clusterDefinition.IsStandalone ?? modelToSave.IsStandalone;

            var agentDefinitions = clusterDefinition.AgentConnections;
            foreach (var agentDefinition in agentDefinitions)
            {
                // Guaranteed by ClusterConfigBuilder.Finish, which registers a config per agent definition.
                var agentConfig = clusterCnnCfg.GetAgentConnectionConfig(agentDefinition.AgentConnectionName);
                
                // ValidateStorageAsync resolves one per agent definition of every cluster.
                // The agent connection id already includes the cluster id.
                if (!State.AgentFingerprints.TryGetValue(agentConfig.Id, out var fingerprint))
                    throw new InvalidOperationException($"Fingerprint not resolved for agent connection {agentDefinition.AgentConnectionName}.");

                // A protected connection fails on ValidateFingerprintAsync
                if (fingerprint.MasterFingerprint != null && fingerprint.MasterFingerprint != fingerprint.AgentFingerprint)
                {
                    logger.Warn(
                        $"Agent connection {agentDefinition.AgentConnectionName} fingerprint has changed, updating...");
                }

                await masterAgentConnectionService.SaveConnectionAsync(new AgentConnectionId(agentConfig.Id),
                    agentConfig.RepositoryTypeId, fingerprint.AgentFingerprint, agentDefinition.ProtectConnectionChanges);
            }

            if (isStandalone)
            {
                var standaloneConfig = clusterCnnCfg.TryGetAgentConnectionConfig(JobMasterConstants.StandaloneAgentConnName);
                if (standaloneConfig != null)
                {
                    var standaloneId = new AgentConnectionId(standaloneConfig.Id);
                    var fingerprintResolver = agentComponentFactory.GetFingerprintResolver(standaloneConfig.Id);
                    var fingerprint = await fingerprintResolver.GiveYourFingerprintAsync(clusterDefinition.ClusterId!, standaloneConfig.Id);
                    await masterAgentConnectionService.SaveConnectionAsync(standaloneId, standaloneConfig.RepositoryTypeId, fingerprint, protectChanges: false);
                }
            }

            if (clusterDefinition.Workers.Any(w => w.Mode.IsFullOr(AgentWorkerMode.Coordinator)))
            {
                var fallbackConfig = clusterCnnCfg.GetAgentConnectionConfig(JobMasterConstants.MasterFallbackAgentConnName);
                var fallbackId = new AgentConnectionId(fallbackConfig.Id);
                var fingerprintResolver = agentComponentFactory.GetFingerprintResolver(fallbackConfig.Id);
                var fingerprint = await fingerprintResolver.GiveYourFingerprintAsync(clusterDefinition.ClusterId!, fallbackConfig.Id);
                await masterAgentConnectionService.SaveConnectionAsync(fallbackId, fallbackConfig.RepositoryTypeId, fingerprint, protectChanges: false);
            }

            var workerDefinitions = clusterDefinition.Workers;

            modelToSave.DefaultJobTimeout = clusterDefinition.DefaultJobTimeout ?? modelToSave.DefaultJobTimeout;
            modelToSave.DefaultMaxOfRetryCount =
                clusterDefinition.DefaultMaxRetryCount ?? modelToSave.DefaultMaxOfRetryCount;
            modelToSave.IanaTimeZoneId = clusterDefinition.IanaTimeZoneId ?? modelToSave.IanaTimeZoneId;
            modelToSave.MaxMessageByteSize = clusterDefinition.MaxMessageByteSize ?? modelToSave.MaxMessageByteSize;
            modelToSave.AdditionalConfig = clusterDefinition.AdditionalConfig ?? modelToSave.AdditionalConfig;
            modelToSave.TransientThreshold = clusterDefinition.TransientThreshold ?? modelToSave.TransientThreshold;
            modelToSave.DataRetentionTtl = clusterDefinition.DataRetentionTtl ?? modelToSave.DataRetentionTtl;
            modelToSave.TargetArchivedClusterId = clusterDefinition.TargetArchivedClusterId ?? modelToSave.TargetArchivedClusterId;
            // Never cleared, only replaced: an archived cluster moves late jobs/schedules there.
            modelToSave.TargetActiveClusterId = clusterDefinition.TargetActiveClusterId ?? modelToSave.TargetActiveClusterId;
            modelToSave.ClusterMode = clusterDefinition.ClusterMode ?? modelToSave.ClusterMode;
            modelToSave.IsStandalone = isStandalone;

            if (clusterCnnCfg.MirrorLog == JsonlFileLogger.LogMirror)
            {
                JsonlFileLogger.AddLogger(clusterCnnCfg.ClusterId, clusterDefinition.MirrorLogFilePath!,
                    clusterDefinition.MirrorLogMaxBufferItems ?? 500, clusterDefinition.MirrorLogFlushInterval);
            }

            // Archived without a target holding unfinished data already failed in ValidateStorageAsync.
            masterConfigService.Save(modelToSave);

            if (modelToSave.ClusterMode == ClusterMode.Archived && !string.IsNullOrEmpty(modelToSave.TargetActiveClusterId))
            {
                // Every worker allowed on an Archived cluster acts as its Coordinator.
                if (!workerDefinitions.Any())
                {
                    logger.Warn(
                        $"Archived cluster '{clusterDefinition.ClusterId}' moves late jobs and recurring schedules to " +
                        $"'{modelToSave.TargetActiveClusterId}' only while a worker of it runs. This app has none;" +
                        "make sure one runs in an app that also configures that cluster.");
                }
                else if (JobMasterClusterAwareComponentFactories.TryGetFactory(modelToSave.TargetActiveClusterId!) == null)
                {
                    logger.Warn(
                        $"Archived cluster '{clusterDefinition.ClusterId}' moves late jobs and recurring schedules to " +
                        $"'{modelToSave.TargetActiveClusterId}', but that cluster isn't configured in this app. " +
                        "Configure it here, next to a worker of the archived cluster, or they stay on the archived cluster.");
                }
            }

            if (!isStandalone)
            {
                var bucketService = componentFactory.GetComponent<IMasterBucketsService>();
                var existingBuckets = await bucketService.QueryAllNoCacheAsync();

                // Create a drainer for standalone buckets. It can happen when a standalone cluster transitions to a non-standalone cluster.
                // workerDefinitions.Any() is to not add in publisher only app instance.
                if (existingBuckets.Any(x => x.IsStandaloneBucket(clusterDefinition.ClusterId!)) &&
                    workerDefinitions.Any())
                {
                    var lanes = existingBuckets.Where(x => x.IsStandaloneBucket(clusterDefinition.ClusterId!))
                        .Select(x => x.WorkerLane)
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .ToList();

                    foreach (var lane in lanes)
                    {
                        var workerDefinition = new WorkerDefinition()
                        {
                            AgentConnectionName = JobMasterConstants.StandaloneAgentConnName,
                            WorkerName = "StandaloneDrainer",
                            WorkerLane = lane,
                            Mode = AgentWorkerMode.Drain,
                            ClusterId = clusterDefinition.ClusterId!,
                            TransferBatchSize = 1000,
                        };

                        var worker = await JobMasterBackgroundAgentWorker.CreateAsync(
                            serviceProvider,
                            workerDefinition);

                        Workers.Add(worker);
                    }
                }
            }

            foreach (var workerDefinition in workerDefinitions)
            {
                var worker = await JobMasterBackgroundAgentWorker.CreateAsync(
                    serviceProvider,
                    workerDefinition);

                Workers.Add(worker);
            }
        }

        foreach (var def in JobMasterClusterConnectionConfig.GetAllConfigs())
        {
            def.MarkAsReady();
        }

        foreach (var worker in Workers)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(JobMasterRandomUtil.GetInt(100, 500)));

            await worker.StartAsync();
        }

        SaveStaticRecurringSchedules(State.StaticSchedules);

        Started = true;
        StartedAt = DateTime.UtcNow;

        BootstrapBlueprintDefinitions.Clear();

        await Task.Delay(TimeSpan.FromSeconds(1));
    }

    private static void ThrowIfFailed(IList<string> validationMessages)
    {
        if (validationMessages.Count > 0)
        {
            throw new InvalidOperationException(
                $"JobMasterRuntime failed validation:{Environment.NewLine}{string.Join(Environment.NewLine, validationMessages)}");
        }
    }
    
    private sealed class StartupState
    {
        public List<string> ValidationMessages { get; } = new List<string>();

        public IReadOnlyList<StaticRecurringScheduleDefinition> StaticSchedules { get; set; } =
            Array.Empty<StaticRecurringScheduleDefinition>();

        // By agent connection id, which already includes the cluster id.
        public Dictionary<string, ResolvedAgentFingerprint> AgentFingerprints { get; } =
            new Dictionary<string, ResolvedAgentFingerprint>(StringComparer.OrdinalIgnoreCase);

        public void Clear()
        {
            ValidationMessages.Clear();
            StaticSchedules = Array.Empty<StaticRecurringScheduleDefinition>();
            AgentFingerprints.Clear();
        }
    }

    /// <summary>An agent connection's fingerprint as saved on master and as resolved at startup.</summary>
    private sealed class ResolvedAgentFingerprint
    {
        public ResolvedAgentFingerprint(string clusterId, string agentConnectionId, string? masterFingerprint, string agentFingerprint)
        {
            ClusterId = clusterId;
            AgentConnectionId = agentConnectionId;
            MasterFingerprint = masterFingerprint;
            AgentFingerprint = agentFingerprint;
        }

        public string ClusterId { get; }
        public string AgentConnectionId { get; }

        /// <summary>The fingerprint saved on master; <c>null</c> when the connection isn't saved yet.</summary>
        public string? MasterFingerprint { get; }

        public string AgentFingerprint { get; }
    }

    /// <summary>
    /// An agent connection whose fingerprint changed while protected, for every cluster. Keeps the master's and the
    /// agent's fingerprint per agent connection in <see cref="State"/> for the save that follows.
    /// </summary>
    private async Task ValidateFingerprintAsync()
    {
        foreach (var clusterCnnCfg in JobMasterClusterConnectionConfig.GetAllConfigs())
        {
            var componentFactory = JobMasterClusterAwareComponentFactories.GetFactory(clusterCnnCfg.ClusterId);
            var agentComponentFactory = componentFactory.GetComponent<IAgentComponentFactory>();
            var masterAgentConnectionService = componentFactory.GetComponent<IMasterAgentConnectionService>();

            var clusterDefinition = BootstrapBlueprintDefinitions.Clusters.Single(c =>
                string.Equals(c.ClusterId, clusterCnnCfg.ClusterId, StringComparison.OrdinalIgnoreCase));

            foreach (var agentDefinition in clusterDefinition.AgentConnections)
            {
                var agentConfig = clusterCnnCfg.GetAgentConnectionConfig(agentDefinition.AgentConnectionName);
                var masterConnection = await masterAgentConnectionService.GetConnectionAsync(
                    new AgentConnectionId(agentConfig.Id), useCache: false);

                var fingerprintResolver = agentComponentFactory.GetFingerprintResolver(agentConfig.Id);
                var agentFingerprint = await fingerprintResolver.GiveYourFingerprintAsync(agentDefinition.ClusterId, agentConfig.Id);

                State.AgentFingerprints[agentConfig.Id] 
                    = new ResolvedAgentFingerprint(clusterCnnCfg.ClusterId, agentConfig.Id, masterConnection?.Fingerprint, agentFingerprint);

                if (agentDefinition.ProtectConnectionChanges &&
                    masterConnection != null &&
                    masterConnection.Fingerprint != agentFingerprint)
                {
                    State.ValidationMessages.Add(
                        $"Agent connection {agentDefinition.AgentConnectionName} fingerprint has changed, " +
                        $"please ensure the connection {agentDefinition.AgentConnectionName} is not modified.");
                }
            }
        }
    }

    /// <summary>
    /// Prevents lost jobs: refuses to start an archived cluster without a target that still holds unfinished jobs or
    /// recurring schedules. An archived cluster runs nothing, and without a target nothing moves them elsewhere, so they
    /// would never run and nobody would be told. With a target, the migrate runners move them to it, so that cluster is
    /// not checked.
    /// Read-only, and runs before provisioning: a schema that doesn't exist yet holds nothing.
    /// </summary>
    private async Task ValidateArchivedStorageAsync()
    {
        foreach (var clusterCnnCfg in JobMasterClusterConnectionConfig.GetAllConfigs())
        {
            var componentFactory = JobMasterClusterAwareComponentFactories.GetFactory(clusterCnnCfg.ClusterId);
            var masterConfigService = componentFactory.GetComponent<IMasterClusterConfigurationService>();

            var clusterDefinition = BootstrapBlueprintDefinitions.Clusters.Single(c =>
                string.Equals(c.ClusterId, clusterCnnCfg.ClusterId, StringComparison.OrdinalIgnoreCase));

            // Same resolution as the save that follows.
            var stored = GetFreshClusterConfig(componentFactory, masterConfigService);
            var clusterMode = clusterDefinition.ClusterMode ?? stored?.ClusterMode;
            var targetActiveClusterId = clusterDefinition.TargetActiveClusterId ?? stored?.TargetActiveClusterId;
            if (clusterMode == ClusterMode.Archived && string.IsNullOrEmpty(targetActiveClusterId))
            {
                var archivedError = await CheckArchivedHoldsOnlyFinalDataAsync(
                    clusterCnnCfg.ClusterId,
                    componentFactory.GetMasterRepository<IMasterJobsRepository>(),
                    componentFactory.GetMasterRepository<IMasterRecurringSchedulesRepository>(),
                    componentFactory.GetComponent<IKnownExceptionIdentifier>());
                if (archivedError != null)
                {
                    State.ValidationMessages.Add(archivedError);
                }
            }
        }
    }

    /// <summary>An error when an archived cluster (without a target) still holds a job or recurring schedule that isn't final.</summary>
    private static async Task<string?> CheckArchivedHoldsOnlyFinalDataAsync(
        string clusterId,
        IMasterJobsRepository jobsRepo,
        IMasterRecurringSchedulesRepository recurringRepo,
        IKnownExceptionIdentifier knownExceptionIdentifier)
    {
        var nonFinalJobStatuses = Enum.GetValues(typeof(JobMasterJobStatus))
            .Cast<JobMasterJobStatus>()
            .Where(s => !s.IsFinalStatus())
            .ToList();

        var hasNonFinalJobs = await AnyUnlessSchemaNotProvisionedAsync(knownExceptionIdentifier, async () =>
            (await jobsRepo.QueryAsync(new JobQueryCriteria { Statuses = nonFinalJobStatuses, CountLimit = 1 })).Any());

        if (hasNonFinalJobs)
        {
            return $"Archived cluster '{clusterId}' contains jobs in a non-final status. " +
                   "Archive clusters without a target may only hold finalized data; use Archive(targetActiveClusterId) to move them.";
        }

        var nonFinalRecurringStatuses = Enum.GetValues(typeof(RecurringScheduleStatus))
            .Cast<RecurringScheduleStatus>()
            .Where(s => !s.IsFinalStatus());

        foreach (var status in nonFinalRecurringStatuses)
        {
            var hasNonFinalSchedules = await AnyUnlessSchemaNotProvisionedAsync(knownExceptionIdentifier, async () =>
                (await recurringRepo.QueryAsync(new RecurringScheduleQueryCriteria { Status = status, CountLimit = 1 })).Any());

            if (hasNonFinalSchedules)
            {
                return $"Archived cluster '{clusterId}' contains recurring schedules in a non-final status. " +
                       "Archive clusters without a target may only hold finalized data; use Archive(targetActiveClusterId) to move them.";
            }
        }

        return null;
    }

    // Like GetFreshClusterConfig: on a first start the table doesn't exist yet, which means nothing is stored.
    private static async Task<bool> AnyUnlessSchemaNotProvisionedAsync(
        IKnownExceptionIdentifier knownExceptionIdentifier,
        Func<Task<bool>> query)
    {
        try
        {
            return await query();
        }
        catch (Exception ex) when (knownExceptionIdentifier.Identify(ex) == JobMasterKnownExceptionId.SchemaNotProvisioned)
        {
            return false;
        }
    }

    /// <summary>
    /// Structural checks of every configured cluster, collected into <paramref name="errors"/> instead of thrown:
    /// <list type="bullet">
    /// <item>a default cluster exists (the only cluster becomes the default when none is marked);</item>
    /// <item>no duplicate cluster ids, agent connection names, worker names or worker lanes;</item>
    /// <item>connection string and repository type set for the cluster and each agent connection;</item>
    /// <item>reserved agent connection names not used; Coordinators without a connection, every other worker on a
    /// declared one;</item>
    /// <item>mode rules: Migrating / Archived without buckets, Archived without agent connections and only Coordinator
    /// (or Full) workers, Migrating without Execution workers, standalone without agent connections;</item>
    /// <item>targets: a Migrating cluster has a <c>MigrateTo</c> target, targets point to a configured cluster in the
    /// right mode, never to itself;</item>
    /// <item>the time zone matches the one saved on master.</item>
    /// </list>
    /// Reads the saved cluster configuration but writes nothing; only sets the default cluster.
    /// </summary>
    private static void ValidateClusterDefinitions(List<string> errors)
    {
        // A single cluster is the default when none is marked. Set here, before the runtime validators: they resolve a
        // handler without its own cluster to the default.
        if (JobMasterClusterConnectionConfig.Default == null)
        {
            if (JobMasterClusterConnectionConfig.ClusterCount == 1)
            {
                JobMasterClusterConnectionConfig.SetDefaultConfig(JobMasterClusterConnectionConfig.GetAllConfigs().Single().ClusterId);
            }
            else if (JobMasterClusterConnectionConfig.ClusterCount == 0)
            {
                errors.Add("No cluster configured. Add one with AddJobMasterCluster(...).");
            }
            else
            {
                errors.Add("Multiple clusters configured but no default cluster defined. Mark one as default.");
            }
        }

        var clusterDupes = JobMasterClusterConnectionConfig
            .GetAllConfigs()
            .GroupBy(x => JobMasterStringUtils.NormalizeId(x.ClusterId)).Where(x => x.Count() > 1)
            .ToList();

        if (clusterDupes.Any())
        {
            errors.Add(
                $"Multiple clusters configured with the same id. {string.Join(", ", clusterDupes.Select(x => x.Key))}");
        }

        // Validation of all clusters first.
        foreach (var clusterCnnCfg in JobMasterClusterConnectionConfig.GetAllConfigs())
        {
            var componentFactory = JobMasterClusterAwareComponentFactories.GetFactory(clusterCnnCfg.ClusterId);
            var clusterDefinition = BootstrapBlueprintDefinitions.Clusters.SingleOrDefault(c =>
                string.Equals(c.ClusterId, clusterCnnCfg.ClusterId, StringComparison.OrdinalIgnoreCase));

            // The checks below need the definition and a reachable master.
            if (clusterDefinition == null)
            {
                errors.Add("Cluster definition not found");
                continue;
            }

            if (string.IsNullOrWhiteSpace(clusterDefinition.ConnString) ||
                string.IsNullOrWhiteSpace(clusterDefinition.RepoType))
            {
                errors.Add("Cluster definition is missing connection string or repository type");
                continue;
            }

            var agentDefinitions = clusterDefinition.AgentConnections;

            var missingCnnStringOrRepoType = agentDefinitions.Where(x =>
                string.IsNullOrWhiteSpace(x.AgentConnString) || string.IsNullOrWhiteSpace(x.AgentRepoType)).ToList();
            if (missingCnnStringOrRepoType.Any())
            {
                var agentNames = string.Join(", ", missingCnnStringOrRepoType.Select(x => x.AgentConnectionName));
                errors.Add(
                    @$"Agent connection is missing connection string or repository type. Connection: {agentNames}");
            }

            var workerDefinitions = clusterDefinition.Workers;

            var masterConfigService = componentFactory.GetComponent<IMasterClusterConfigurationService>();

            var existingClusterConfig = GetFreshClusterConfig(componentFactory, masterConfigService);
            var existingTimezoneId = existingClusterConfig?.IanaTimeZoneId ?? TimeZoneUtils.GetLocalIanaTimeZoneId();

            // clusterDefinition.IsStandalone is null when not set in code (e.g. a cluster configured as
            // standalone purely through ConfigFromJson in a prior run) — mirrors the same resolution
            // StartAsync uses later (clusterDefinition.IsStandalone ?? modelToSave.IsStandalone).
            var isStandalone = clusterDefinition.IsStandalone ?? existingClusterConfig?.IsStandalone ?? false;

            if (clusterDefinition.IanaTimeZoneId != null &&
                existingTimezoneId != TimeZoneUtils.GetLocalIanaTimeZoneId())
            {
                errors.Add(
                    "if you want to use agents in different regions please explicitly set the IanaTimeZoneId for the cluster. " +
                    "The cluster IanaTimeZoneId does not match the local timezone" +
                    " ClusterId: " + clusterDefinition.ClusterId + ", Defined IanaTimeZoneId: " +
                    clusterDefinition.IanaTimeZoneId + ", Existing IanaTimeZoneId Configured: " + existingTimezoneId);
            }

            // Ensure no duplicates
            var agentDupes = agentDefinitions
                .GroupBy(x => JobMasterStringUtils.NormalizeId(x.AgentConnectionName))
                .Where(x => x.ToList().Count > 1).ToList();
            if (agentDupes.Any())
            {
                errors.Add(
                    $"Duplicate agent connection names found. {string.Join(", ", agentDupes.Select(x => x.Key))}");
            }

            if ((clusterDefinition.ClusterMode == ClusterMode.Migrating ||
                 clusterDefinition.ClusterMode == ClusterMode.Archived) &&
                workerDefinitions.Any(x => x.BucketQty.Any(y => y.Value >= 1)))
            {
                errors.Add("Migrating and Archived clusters can not have buckets defined");
            }

            if (clusterDefinition.ClusterMode == ClusterMode.Archived && agentDefinitions.Any())
            {
                errors.Add(
                    $"Archived cluster '{clusterDefinition.ClusterId}' cannot have agent connections defined. " +
                    "Archive clusters only run Coordinator (or Full, which behaves identically there) workers.");
            }

            // Full behaves exactly like Coordinator on an Archived cluster (no agent connection, same
            // runner set) — see JobMasterBackgroundAgentWorker.StartAsync/StartCoordinatorAsync. Only
            // Execution/Drain are actually incompatible with an Archived cluster.
            if (clusterDefinition.ClusterMode == ClusterMode.Archived &&
                workerDefinitions.Any(x => !x.Mode.IsFullOr(AgentWorkerMode.Coordinator)))
            {
                errors.Add(
                    $"Archived cluster '{clusterDefinition.ClusterId}' may only have Coordinator.");
            }

            // Execution is the only mode incompatible with Migrating: Coordinator and Drain are the
            // point of the mode, and Full is allowed too (it skips its execution runners there and acts
            // as Coordinator+Drain — see JobMasterBackgroundAgentWorker.LoadFullRunnersAsync).
            if (clusterDefinition.ClusterMode == ClusterMode.Migrating &&
                workerDefinitions.Any(x => x.Mode == AgentWorkerMode.Execution))
            {
                errors.Add(
                    $"Migrating cluster '{clusterDefinition.ClusterId}' may not have Execution-mode workers.");
            }

            if (!string.IsNullOrEmpty(clusterDefinition.TargetArchivedClusterId))
            {
                if (string.Equals(clusterDefinition.TargetArchivedClusterId, clusterDefinition.ClusterId, StringComparison.OrdinalIgnoreCase))
                {
                    errors.Add(
                        $"Cluster '{clusterDefinition.ClusterId}' cannot use itself as its own TargetArchivedClusterId.");
                }

                var targetClusterDefinition = BootstrapBlueprintDefinitions.Clusters.SingleOrDefault(c =>
                    string.Equals(c.ClusterId, clusterDefinition.TargetArchivedClusterId, StringComparison.OrdinalIgnoreCase));

                if (targetClusterDefinition == null)
                {
                    errors.Add(
                        $"Cluster '{clusterDefinition.ClusterId}' has TargetArchivedClusterId '{clusterDefinition.TargetArchivedClusterId}' " +
                        "which does not match any configured cluster.");
                }
                else if (targetClusterDefinition.ClusterMode != ClusterMode.Archived)
                {
                    errors.Add(
                        $"Cluster '{clusterDefinition.ClusterId}' has TargetArchivedClusterId '{clusterDefinition.TargetArchivedClusterId}', " +
                        "but that cluster is not configured with ClusterMode.Archived.");
                }
            }

            if (clusterDefinition.ClusterMode == ClusterMode.Migrating && string.IsNullOrEmpty(clusterDefinition.TargetActiveClusterId))
            {
                errors.Add(
                    $"Migrating cluster '{clusterDefinition.ClusterId}' must have a TargetActiveClusterId configured (use MigrateTo(...)).");
            }

            if (!string.IsNullOrEmpty(clusterDefinition.TargetActiveClusterId) &&
                clusterDefinition.ClusterMode != ClusterMode.Migrating &&
                clusterDefinition.ClusterMode != ClusterMode.Archived)
            {
                errors.Add(
                    $"Cluster '{clusterDefinition.ClusterId}' has a TargetActiveClusterId configured but is not in ClusterMode.Migrating or ClusterMode.Archived. " +
                    "TargetActiveClusterId only applies to Migrating and Archived clusters (use MigrateTo(...) or Archive(...) instead of setting it directly).");
            }

            if (!string.IsNullOrEmpty(clusterDefinition.TargetActiveClusterId))
            {
                if (string.Equals(clusterDefinition.TargetActiveClusterId, clusterDefinition.ClusterId, StringComparison.OrdinalIgnoreCase))
                {
                    errors.Add(
                        $"Cluster '{clusterDefinition.ClusterId}' cannot use itself as its own TargetActiveClusterId.");
                }

                var targetClusterDefinition = BootstrapBlueprintDefinitions.Clusters.SingleOrDefault(c =>
                    string.Equals(c.ClusterId, clusterDefinition.TargetActiveClusterId, StringComparison.OrdinalIgnoreCase));

                if (targetClusterDefinition == null)
                {
                    errors.Add(
                        $"Cluster '{clusterDefinition.ClusterId}' has TargetActiveClusterId '{clusterDefinition.TargetActiveClusterId}' " +
                        "which does not match any configured cluster.");
                }
                // Active is the framework default, so a target that never calls Mode(...) at all is
                // still validly Active — unlike TargetArchivedClusterId's check, a null mode here is not
                // an error.
                else if ((targetClusterDefinition.ClusterMode ?? ClusterMode.Active) != ClusterMode.Active)
                {
                    errors.Add(
                        $"Cluster '{clusterDefinition.ClusterId}' has TargetActiveClusterId '{clusterDefinition.TargetActiveClusterId}', " +
                        "but that cluster is not configured with ClusterMode.Active.");
                }
            }

            if (agentDefinitions.Any(x =>
                    JobMasterConstants.IsReservedAgentConnectionName(x.AgentConnectionName)))
            {
                errors.Add(
                    "One or more agent connections use a reserved agent connection name.");
            }

            if (workerDefinitions.Any(x => string.Equals(
                    x.AgentConnectionName,
                    JobMasterConstants.MasterFallbackAgentConnName,
                    StringComparison.OrdinalIgnoreCase)))
            {
                errors.Add(
                    $"{JobMasterConstants.MasterFallbackAgentConnName} is reserved for fallback buckets. Cannot be used as a worker's AgentConnectionName.");
            }

            if (!isStandalone && workerDefinitions.Any(x => string.Equals(
                    x.AgentConnectionName,
                    JobMasterConstants.StandaloneAgentConnName,
                    StringComparison.OrdinalIgnoreCase)))
            {
                errors.Add(
                    $"{JobMasterConstants.StandaloneAgentConnName} is reserved for standalone clusters. Cannot be used as a worker's AgentConnectionName in a non-standalone cluster.");
            }

            if (workerDefinitions.Any(x => ActsAsCoordinator(x, clusterDefinition) && !string.IsNullOrEmpty(x.AgentConnectionName)))
            {
                errors.Add(
                    "Coordinator workers (and Full workers on an Archived cluster) must not have an AgentConnectionName configured.");
            }

            if (!isStandalone)
            {
                // Standalone workers always get AgentConnectionName forced to StandaloneAgentConnName by the
                // builder, so there's nothing to cross-reference there — only check explicit clusters.
                var invalidWorkers = workerDefinitions
                    .Where(x => !ActsAsCoordinator(x, clusterDefinition))
                    .Where(x => !agentDefinitions.Any(a => string.Equals(a.AgentConnectionName, x.AgentConnectionName, StringComparison.OrdinalIgnoreCase)))
                    .Select(x => string.IsNullOrEmpty(x.WorkerName) ? "(unnamed)" : x.WorkerName)
                    .ToList();

                if (invalidWorkers.Any())
                {
                    errors.Add(
                        $"The following workers have an AgentConnectionName that does not match any registered agent connection: {string.Join(", ", invalidWorkers)}");
                }
            }

            if (isStandalone && agentDefinitions.Any())
            {
                errors.Add(
                    "Standalone clusters cannot have agents defined. The standalone stays in the master db together with the cluster");
            }


            // Ensure no duplicates
            var workDupes = workerDefinitions
                .Where(x => !string.IsNullOrEmpty(x.WorkerName))
                .GroupBy(x => JobMasterStringUtils.NormalizeId(x.WorkerName!)).Where(x => x.ToList().Count > 1)
                .ToList();
            if (workDupes.Any())
            {
                errors.Add(
                    $"Duplicate worker names found {string.Join(", ", workDupes.Select(x => x.Key))}");
            }

            var distinctWorkLanes = workerDefinitions
                .Where(x => !string.IsNullOrEmpty(x.WorkerLane))
                .Select(x => x.WorkerLane)
                .Distinct()
                .ToList();

            var workLanesDupes = distinctWorkLanes.GroupBy(x => JobMasterStringUtils.NormalizeId(x!))
                .Where(x => x.ToList().Count > 1).ToList();
            if (workLanesDupes.Any())
            {
                errors.Add(
                    $"Duplicate worker lanes found {string.Join(", ", workLanesDupes.Select(x => x.Key))}");
            }

            // Handler priorities vs DisabledPriorities: per handler cluster, in DefaultRuntimeValidatorSetup.
        }
    }

    // A Full worker on an Archived cluster behaves exactly like Coordinator (no connection at all --
    // see JobMasterBackgroundAgentWorker.StartAsync), so it shares the same exemptions used by
    // ValidateClusterDefinitions' AgentConnectionName checks.
    private static bool ActsAsCoordinator(WorkerDefinition w, ClusterDefinition clusterDefinition) =>
        w.Mode == AgentWorkerMode.Coordinator ||
        (clusterDefinition.ClusterMode == ClusterMode.Archived && w.Mode == AgentWorkerMode.Full);

    /// <summary>
    /// Reads the cluster's previously-saved configuration, if any -- used by <see cref="ValidateClusterDefinitions"/>
    /// to check cross-run consistency (timezone, IsStandalone, etc.) before schema auto-provisioning
    /// (<c>OnBeforeStartAsync</c>, which runs strictly after all validation) has had a chance to run.
    /// On a cluster's very first startup against a brand-new database, the underlying table doesn't
    /// exist yet -- that's equivalent to "no config saved yet", not a real failure, so it's treated the
    /// same as a genuinely empty table instead of letting the provider-specific SQL exception abort
    /// startup before provisioning ever gets a chance to run.
    /// </summary>
    private static ClusterConfigurationModel? GetFreshClusterConfig(
        IJobMasterClusterAwareComponentFactory componentFactory,
        IMasterClusterConfigurationService masterConfigService)
    {
        var knownExceptionIdentifier = componentFactory.GetComponent<IKnownExceptionIdentifier>();
        try
        {
            return masterConfigService.GetFresh();
        }
        catch (Exception ex) when (knownExceptionIdentifier.Identify(ex) == JobMasterKnownExceptionId.SchemaNotProvisioned)
        {
            return null;
        }
    }

    public int CountWorkersForCluster(string clusterId)
    {
        if (string.IsNullOrEmpty(clusterId))
        {
            return 0;
        }

        return Workers.Count(w => w.ClusterConnConfig.ClusterId == clusterId);
    }

    public void Stop()
    {
        List<JobMasterBackgroundAgentWorker> toStop;
        toStop = new List<JobMasterBackgroundAgentWorker>(Workers);
        Workers.Clear();

        foreach (var w in toStop)
        {
            try
            {
                w.RequestStop();
                w.Dispose();
            }
            catch
            {
                // Swallow to attempt stopping others; consider logging hook later
            }
        }
    }

    // Built and validated by StaticRecurringScheduleDefinitionsBuilder before anything started; this only saves them.
    private static void SaveStaticRecurringSchedules(IReadOnlyList<StaticRecurringScheduleDefinition> definitions)
    {
        // Upsert each desired (one-by-one)
        foreach (var byCluster in definitions.GroupBy(x => x.ClusterId, StringComparer.OrdinalIgnoreCase))
        {
            var clusterId = byCluster.Key;
            var componentFactory = JobMasterClusterAwareComponentFactories.GetFactory(clusterId);
            var masterRecurringService = componentFactory.GetService<IMasterRecurringSchedulesService>();
            var masterDistributedLockerService = componentFactory.GetService<IMasterDistributedLockerService>();
            var jobMasterLockKeys = new JobMasterLockKeys(clusterId);
            foreach (var config in byCluster)
            {
                var lockKey = jobMasterLockKeys.RecurringScheduleUpsertStatic(config.Id);
                var lockToken = masterDistributedLockerService.TryLock(lockKey, TimeSpan.FromMinutes(1));
                if (lockToken == null)
                {
                    continue;
                }

                try
                {
                    masterRecurringService.UpsertStatic(config);
                    StaticRecurringDefinitionIdsKeeper.Add(clusterId, config.Id);
                }
                finally
                {
                    masterDistributedLockerService.ReleaseLock(lockKey, lockToken);
                }
            }
        }
    }
}
