using JobMaster.Abstractions.Ioc.Selectors;

namespace JobMaster.Sdk.Abstractions.Ioc;

internal interface IConnectionOptionsBinder
{
    string RepoType { get; }
    void SetOptions(IAgentConnectionProviderSelector selector, IDictionary<string, object> options);
    void SetOptions(IClusterConfigSelector selector, IDictionary<string, object> options);
}
