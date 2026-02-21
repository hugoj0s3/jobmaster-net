using JobMaster.Sdk.Abstractions.Ioc.Markups;

namespace JobMaster.Sdk.Abstractions.Services;

internal interface IRandomFriendlyNameService : IJobMasterClusterAwareService
{
    string GetRandomFriendlyName(bool includeAdjective = true);
}