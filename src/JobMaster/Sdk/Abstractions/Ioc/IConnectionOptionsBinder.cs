using JobMaster.Abstractions.Ioc.Selectors;

namespace JobMaster.Sdk.Abstractions.Ioc;

internal interface IConnectionOptionsBinder
{
    string RepoType { get; }
    void SetOptions(IAgentConnectionConfigSelector selector, IDictionary<string, object> options);
    void SetOptions(IClusterConfigSelector selector, IDictionary<string, object> options);
}
