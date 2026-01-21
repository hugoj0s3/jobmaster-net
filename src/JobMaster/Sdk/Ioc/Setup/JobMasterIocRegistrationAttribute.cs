using System.Reflection;
using JobMaster.Sdk.Abstractions.Ioc;

namespace JobMaster.Sdk.Ioc.Setup;

/// <summary>
/// Marks a class as a JobMaster IoC registration module.
/// The class must implement two static methods:
/// - public static string RepositoryType { get; }
/// - public static void RegisterForMaster(IServiceCollection services)
/// - public static void RegisterForAgent(IServiceCollection services)
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class JobMasterIocRegistrationAttribute : Attribute
{
    private const string RepositoryTypeProperty = "RepositoryType";
    private const string RegisterForMasterMethod = "RegisterForMaster";
    private const string RegisterForAgentMethod = "RegisterForAgent";

    /// <summary>
    /// Registers all services from modules matching the specified repository type
    /// </summary>
    public static void RegisterAllForMaster(IClusterIocRegistration clusterIocRegistration, string repositoryType, string clusterId) 
    { 
        foreach (var type in GetRegistrationTypes())
        {
            try
            {
                // Check repository type
                var repoTypeProp = type.GetProperty(RepositoryTypeProperty, 
                    BindingFlags.Public | BindingFlags.Static);
                
                if (repoTypeProp?.GetValue(null)?.ToString() != repositoryType)
                    continue;

                // Invoke the appropriate registration method
                var method = type.GetMethod(RegisterForMasterMethod, 
                    BindingFlags.Public | BindingFlags.Static);
                
                if (method == null)
                    continue;
                
                method.Invoke(null, new object[] { clusterIocRegistration, clusterId });
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Failed to register services from {type.FullName}", ex);
            }
        }
    }
    
    public static void RegisterAllForAgent(IClusterIocRegistration clusterIocRegistration, string repositoryType, string clusterId)
    {
        foreach (var type in GetRegistrationTypes())
        {
            try
            {
                // Check repository type
                var repoTypeProp = type.GetProperty(RepositoryTypeProperty, 
                    BindingFlags.Public | BindingFlags.Static);
                
                if (repoTypeProp?.GetValue(null)?.ToString() != repositoryType)
                    continue;

                // Invoke the appropriate registration method
                var method = type.GetMethod(RegisterForAgentMethod, 
                    BindingFlags.Public | BindingFlags.Static);
                
                if (method == null)
                    continue;
                
                method.Invoke(null, new object[] { clusterIocRegistration, clusterId });
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Failed to register services from {type.FullName}", ex);
            }
        }
    }

    private static IEnumerable<Type> GetRegistrationTypes()
    {
        return AppDomain.CurrentDomain.GetAssemblies()
            .SelectMany(a => a.GetTypes())
            .Where(t => t.GetCustomAttribute<JobMasterIocRegistrationAttribute>() != null);
    }
}
