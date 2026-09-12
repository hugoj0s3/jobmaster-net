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
        if (!JobMasterRuntimeSingleton.Instance.Started)
        {
            return NotStartedThrottler;
        }

        return InternalAgentThrottlers.GetOrAdd(agentConnectionId, _ =>
            OperationThrottlerSettingsTemplateFactory.GetInternalAgentThrottlerTemplate(AgentRepositoryTypeId(agentConnectionId)).Create());
    }

    public static OperationThrottler GetSchedulingAgentThrottler(string agentConnectionId)
    {
        if (!JobMasterRuntimeSingleton.Instance.Started)
        {
            return NotStartedThrottler;
        }

        return SchedulingAgentThrottlers.GetOrAdd(agentConnectionId, _ =>
            OperationThrottlerSettingsTemplateFactory.GetSchedulingAgentThrottlerTemplate(AgentRepositoryTypeId(agentConnectionId)).Create());
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
    {
        if (!JobMasterRuntimeSingleton.Instance.Started)
        {
            return NotStartedMaxBatchSize;
        }

        return OperationThrottlerSettingsTemplateFactory.GetAgentMaxBatchSize(AgentRepositoryTypeId(agentConnectionId));
    }

    private static string MasterRepositoryTypeId(string clusterId) =>
        JobMasterClusterConnectionConfig.Get(clusterId, includeNotReady: true).RepositoryTypeId;

    private static string AgentRepositoryTypeId(string agentConnectionId)
    {
        var id = new AgentConnectionId(agentConnectionId);
        return JobMasterClusterConnectionConfig.Get(id.ClusterId, includeNotReady: true)
            .GetAgentConnectionConfig(id.Name).RepositoryTypeId;
    }
}
