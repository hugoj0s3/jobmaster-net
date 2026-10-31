using System.Reflection;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Reflection;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Ioc.Setup;

/// <summary>
/// Marks a class as a JobMaster IoC registration module.
/// The class must implement two static methods:
/// - public static string RepositoryType { get; }
/// - public static void RegisterForMaster(IServiceCollection services)
/// - public static void RegisterForAgent(IServiceCollection services)
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
internal sealed class JobMasterIocRegistrationAttribute : Attribute
{
    private const string RepositoryTypeProperty = "RepositoryType";
    private const string RegisterForMasterMethod = "RegisterForMaster";
    private const string RegisterForAgentMethod = "RegisterForAgent";

    /// <summary>
    /// Registers all services from modules matching the specified repository type
    /// </summary>
    public static void RegisterProviderExtensionsForMaster(IClusterIocRegistration clusterIocRegistration, string repositoryType, string clusterId)
    {
        RegisterProviderExtensions(clusterIocRegistration, repositoryType, clusterId, RegisterForMasterMethod, "master");
    }

    public static void RegisterProviderExtensionsForAgent(IClusterIocRegistration clusterIocRegistration, string repositoryType, string clusterId)
    {
        RegisterProviderExtensions(clusterIocRegistration, repositoryType, clusterId, RegisterForAgentMethod, "agent");
    }

    private static void RegisterProviderExtensions(
        IClusterIocRegistration clusterIocRegistration,
        string repositoryType,
        string clusterId,
        string registerMethodName,
        string role)
    {
        ProviderAssemblies.EnsureLoaded(repositoryType);

        var providerFound = false;
        var registered = 0;
        foreach (var type in GetRegistrationTypes())
        {
            try
            {
                // Check repository type
                var repoTypeProp = type.GetProperty(RepositoryTypeProperty,
                    BindingFlags.Public | BindingFlags.Static);

                if (repoTypeProp?.GetValue(null)?.ToString() != repositoryType)
                    continue;

                providerFound = true;

                // Invoke the appropriate registration method
                var method = type.GetMethod(registerMethodName,
                    BindingFlags.Public | BindingFlags.Static);

                if (method == null)
                    continue;

                method.Invoke(null, new object[] { clusterIocRegistration, clusterId });
                registered++;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Failed to register services from {type.FullName}", ex);
            }
        }

        if (registered == 0)
        {
            throw new InvalidOperationException(providerFound
                ? $"The '{repositoryType}' provider can't be used as {role} (cluster '{clusterId}')."
                : $"No JobMaster provider found for repository type '{repositoryType}' (cluster '{clusterId}'). " +
                  ProviderAssemblies.MissingProviderHint(repositoryType));
        }
    }

    private static IEnumerable<Type> GetRegistrationTypes()
    {
        return TypeDiscovery.AllTypes()
            .Where(t => t.GetCachedAttribute<JobMasterIocRegistrationAttribute>() != null);
    }
}
