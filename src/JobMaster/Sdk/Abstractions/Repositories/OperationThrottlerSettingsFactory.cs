using System.Collections.Concurrent;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Agents;

namespace JobMaster.Sdk.Abstractions.Repositories;

/// <summary>
/// Facade over <see cref="OperationThrottlerSettingsTemplateFactory"/> -- callers only ever have a
/// connection identity (a <c>clusterId</c> for the master role, an <c>agentConnectionId</c> for the
/// two agent-facing roles), never a repository type id in hand, so this class resolves that
/// internally (via <see cref="JobMasterClusterConnectionConfig"/>) before delegating to the template
/// factory, instead of every caller repeating that resolution itself.
/// </summary>
internal static class OperationThrottlerSettingsFactory
{
    // Per-connection throttler caches -- ConcurrentDictionary because, unlike
    // OperationThrottlerSettingsTemplateFactory's registry (registered once at startup before
    // Started), these are read AND written during live request handling. Keyed by connection
    // identity (clusterId / agentConnectionId), never by repositoryTypeId, so two connections of
    // the same repository type never share one OperationThrottler/semaphore.
    private static readonly ConcurrentDictionary<string, OperationThrottler> MasterThrottlers = new();
    private static readonly ConcurrentDictionary<string, OperationThrottler> InternalAgentThrottlers = new();
    private static readonly ConcurrentDictionary<string, OperationThrottler> SchedulingAgentThrottlers = new();
    private static readonly ConcurrentDictionary<string, OperationThrottler> BucketLifecycleThrottlers = new();

    private static readonly OperationThrottler NotStartedThrottler = new(3, 250);
    private const int NotStartedMaxBatchSize = 10;

    public static OperationThrottler GetMasterThrottler(string clusterId)
    {
        if (!JobMasterRuntimeSingleton.Instance.Started)
        {
            return NotStartedThrottler;
        }

        return MasterThrottlers.GetOrAdd(clusterId, _ =>
            OperationThrottlerSettingsTemplateFactory.GetMasterThrottlerTemplate(MasterRepositoryTypeId(clusterId)).Create());
    }

    public static OperationThrottler GetInternalAgentThrottler(string agentConnectionId)
    {
        return GetInternalAgentTransportThrottler(agentConnectionId);
    }

    public static OperationThrottler GetInternalAgentTransportThrottler(string agentConnectionId)
    {
        if (!JobMasterRuntimeSingleton.Instance.Started)
        {
            return NotStartedThrottler;
        }

        var config = ResolveAgentConfig(agentConnectionId);
        var cacheKey = config.IsHybrid ? $"{agentConnectionId}:transport" : agentConnectionId;
        return InternalAgentThrottlers.GetOrAdd(cacheKey, _ =>
            OperationThrottlerSettingsTemplateFactory.GetInternalAgentThrottlerTemplate(config.TransportConfig.RepositoryTypeId).Create());
    }

    public static OperationThrottler GetInternalAgentExecutionThrottler(string agentConnectionId)
    {
        if (!JobMasterRuntimeSingleton.Instance.Started)
        {
            return NotStartedThrottler;
        }

        var config = ResolveAgentConfig(agentConnectionId);
        var cacheKey = config.IsHybrid ? $"{agentConnectionId}:execution" : agentConnectionId;
        return InternalAgentThrottlers.GetOrAdd(cacheKey, _ =>
            OperationThrottlerSettingsTemplateFactory.GetInternalAgentThrottlerTemplate(config.ExecutionConfig.RepositoryTypeId).Create());
    }

    public static OperationThrottler GetSchedulingAgentThrottler(string agentConnectionId)
    {
        if (!JobMasterRuntimeSingleton.Instance.Started)
        {
            return NotStartedThrottler;
        }

        return SchedulingAgentThrottlers.GetOrAdd(agentConnectionId, _ =>
        {
            var config = ResolveAgentConfig(agentConnectionId);
            return OperationThrottlerSettingsTemplateFactory.GetSchedulingAgentThrottlerTemplate(config.TransportConfig.RepositoryTypeId).Create();
        });
    }

    // One at a time per agent connection, for the rare bucket operations that touch both legs
    // (HasJobs / CreateBucket / DestroyBucket). Same for every provider.
    public static OperationThrottler GetBucketLifecycleThrottler(string agentConnectionId)
    {
        if (!JobMasterRuntimeSingleton.Instance.Started)
        {
            return NotStartedThrottler;
        }

        return BucketLifecycleThrottlers.GetOrAdd(agentConnectionId, _ => new OperationThrottler(1, 1000));
    }

    public static int GetMasterMaxBatchSize(string clusterId)
    {
        if (!JobMasterRuntimeSingleton.Instance.Started)
        {
            return NotStartedMaxBatchSize;
        }

        return OperationThrottlerSettingsTemplateFactory.GetMasterMaxBatchSize(MasterRepositoryTypeId(clusterId));
    }

    public static int GetAgentMaxBatchSize(string agentConnectionId)
        => GetAgentTransportMaxBatchSize(agentConnectionId);

    public static int GetAgentTransportMaxBatchSize(string agentConnectionId)
    {
        if (!JobMasterRuntimeSingleton.Instance.Started)
        {
            return NotStartedMaxBatchSize;
        }

        var config = ResolveAgentConfig(agentConnectionId);
        return OperationThrottlerSettingsTemplateFactory.GetAgentMaxBatchSize(config.TransportConfig.RepositoryTypeId);
    }

    public static int GetAgentExecutionMaxBatchSize(string agentConnectionId)
    {
        if (!JobMasterRuntimeSingleton.Instance.Started)
        {
            return NotStartedMaxBatchSize;
        }

        var config = ResolveAgentConfig(agentConnectionId);
        return OperationThrottlerSettingsTemplateFactory.GetAgentMaxBatchSize(config.ExecutionConfig.RepositoryTypeId);
    }

    private static string MasterRepositoryTypeId(string clusterId) =>
        JobMasterClusterConnectionConfig.Get(clusterId, includeNotReady: true).RepositoryTypeId;

    private static JobMasterAgentConnectionConfig ResolveAgentConfig(string agentConnectionId)
    {
        var id = new AgentConnectionId(agentConnectionId);
        return JobMasterClusterConnectionConfig.Get(id.ClusterId, includeNotReady: true)
            .GetAgentConnectionConfig(id.Name);
    }
}
