using System.Reflection;

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
        var assemblies = AppDomain.CurrentDomain
            .GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location));

        var runtimeType = assemblies
            .SelectMany(a =>
            {
                try { return a.GetTypes(); }
                catch (ReflectionTypeLoadException ex) { return ex.Types.Where(t => t != null)!; }
            })
            .FirstOrDefault(t =>
                t != null &&
                typeof(IJobMasterRuntime).IsAssignableFrom(t) &&
                !t.IsInterface &&
                !t.IsAbstract);

        if (runtimeType == null)
            throw new InvalidOperationException("No implementation of IJobMasterRuntime found");
        
        return  Activator.CreateInstance(runtimeType) as IJobMasterRuntime;
    }
}