using JobMaster.Sdk.Abstractions.Repositories.Agent;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Sdk.Ioc;

internal static class ClusterServiceProviderExtensions
{
    public static IAgentRawMessagesDispatcherRepository GetAgentRawMessagesSavingRepository(
        this IServiceProvider sp, string repositoryTypeId)
    {
        var key = ClusterServiceKeys.GetAgentRawJobsDispatcherSavingKey(repositoryTypeId);
        return sp.GetRequiredKeyedService<IAgentRawMessagesDispatcherRepository>(key);
    }

    public static IAgentRawMessagesDispatcherRepository GetAgentRawMessagesProcessingRepository(
        this IServiceProvider sp, string repositoryTypeId)
    {
        var key = ClusterServiceKeys.GetAgentRawJobsDispatcherProcessingKey(repositoryTypeId);
        return sp.GetRequiredKeyedService<IAgentRawMessagesDispatcherRepository>(key);
    }

    public static IAgentRawFingerprintResolver GetAgentRawFingerprintResolver(
        this IServiceProvider sp, string repositoryTypeId)
    {
        var key = ClusterServiceKeys.GetFingerprintResolverKey(repositoryTypeId);
        return sp.GetRequiredKeyedService<IAgentRawFingerprintResolver>(key);
    }
}
