namespace JobMaster.Sdk.Abstractions.Repositories;

internal sealed class OperationThrottlerSettingsTemplate
{
    private readonly int? capacity;
    private readonly int acquireTimeoutMs;

    public OperationThrottlerSettingsTemplate(int? capacity, int acquireTimeoutMs = 10000)
    {
        this.capacity = capacity;
        this.acquireTimeoutMs = acquireTimeoutMs;
    }

    public OperationThrottler Create() => new OperationThrottler(capacity, acquireTimeoutMs);
}

internal static class OperationThrottlerSettingsTemplateFactory
{
    private sealed class MasterEntry
    {
        public MasterEntry(int maxBatchSize, OperationThrottlerSettingsTemplate throttler)
        {
            MaxBatchSize = maxBatchSize;
            Throttler = throttler;
        }

        public int MaxBatchSize { get; }
        public OperationThrottlerSettingsTemplate Throttler { get; }
    }

    private sealed class AgentEntry
    {
        public AgentEntry(int maxBatchSize, OperationThrottlerSettingsTemplate internalThrottler, OperationThrottlerSettingsTemplate schedulingThrottler)
        {
            MaxBatchSize = maxBatchSize;
            InternalThrottler = internalThrottler;
            SchedulingThrottler = schedulingThrottler;
        }

        public int MaxBatchSize { get; }
        public OperationThrottlerSettingsTemplate InternalThrottler { get; }
        public OperationThrottlerSettingsTemplate SchedulingThrottler { get; }
    }

    private static readonly Dictionary<string, MasterEntry> MasterEntries = new();
    private static readonly Dictionary<string, AgentEntry> AgentEntries = new();

    public static void RegisterForMaster(
        string repositoryTypeId,
        int maxBatchSize,
        OperationThrottlerSettingsTemplate throttlerSettingsTemplate)
    {
        if (JobMasterRuntimeSingleton.Instance.Started)
        {
            throw new InvalidOperationException("Cannot register repository throttle settings after JobMaster is started.");
        }

        MasterEntries[repositoryTypeId] = new MasterEntry(maxBatchSize, throttlerSettingsTemplate);
    }

    public static void RegisterForAgent(
        string repositoryTypeId,
        int maxBatchSize,
        OperationThrottlerSettingsTemplate internalThrottlerSettingsTemplate,
        OperationThrottlerSettingsTemplate schedulingThrottlerSettingsTemplate)
    {
        if (JobMasterRuntimeSingleton.Instance.Started)
        {
            throw new InvalidOperationException("Cannot register repository throttle settings after JobMaster is started.");
        }

        AgentEntries[repositoryTypeId] = new AgentEntry(maxBatchSize, internalThrottlerSettingsTemplate, schedulingThrottlerSettingsTemplate);
    }

    private static MasterEntry GetMasterEntry(string repositoryTypeId) =>
        MasterEntries.TryGetValue(repositoryTypeId, out var entry)
            ? entry
            : MasterEntries[repositoryTypeId] = new MasterEntry(50, new OperationThrottlerSettingsTemplate(50));

    private static AgentEntry GetAgentEntry(string repositoryTypeId) =>
        AgentEntries.TryGetValue(repositoryTypeId, out var entry)
            ? entry
            : AgentEntries[repositoryTypeId] = new AgentEntry(50, new OperationThrottlerSettingsTemplate(25), new OperationThrottlerSettingsTemplate(10, 500));

    public static int GetMasterMaxBatchSize(string repositoryTypeId) => GetMasterEntry(repositoryTypeId).MaxBatchSize;
    public static int GetAgentMaxBatchSize(string repositoryTypeId) => GetAgentEntry(repositoryTypeId).MaxBatchSize;

    // Exposed only for OperationThrottlerSettingsFactory to mint live per-connection throttlers from --
    // everything else should go through GetMasterMaxBatchSize/GetAgentMaxBatchSize above, or through
    // OperationThrottlerSettingsFactory for an actual OperationThrottler.
    internal static OperationThrottlerSettingsTemplate GetMasterThrottlerTemplate(string repositoryTypeId) => GetMasterEntry(repositoryTypeId).Throttler;
    internal static OperationThrottlerSettingsTemplate GetInternalAgentThrottlerTemplate(string repositoryTypeId) => GetAgentEntry(repositoryTypeId).InternalThrottler;
    internal static OperationThrottlerSettingsTemplate GetSchedulingAgentThrottlerTemplate(string repositoryTypeId) => GetAgentEntry(repositoryTypeId).SchedulingThrottler;
}
