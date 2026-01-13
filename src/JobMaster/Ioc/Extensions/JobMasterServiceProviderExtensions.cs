using System.Reflection;
using JobMaster.Contracts.Extensions;
using JobMaster.Sdk.Contracts;

namespace JobMaster.Ioc.Extensions;

public static class JobMasterServiceProviderExtensions
{ 
    public static async Task StartJobMasterRuntimeAsync(this IServiceProvider serviceProvider)
    {
        await JobMasterRuntimeSingleton.Instance.StartAsync(serviceProvider);
    }
}
