using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Abstractions;

internal static class JobMasterRuntimeSingleton
{
    private static IJobMasterRuntime? _instance;
    public static IJobMasterRuntime Instance
    {
        get
        {
            return _instance ??= FindRuntimeImpl() ?? throw new InvalidOperationException("No implementation of IJobMasterRuntime found");
        }
    }

    private static IJobMasterRuntime? FindRuntimeImpl()
    {
        var runtimeType = typeof(IJobMasterRuntime).GetCachedConcreteTypesOf().FirstOrDefault();

        if (runtimeType == null)
            throw new InvalidOperationException("No implementation of IJobMasterRuntime found");

        return  Activator.CreateInstance(runtimeType) as IJobMasterRuntime;
    }
}
