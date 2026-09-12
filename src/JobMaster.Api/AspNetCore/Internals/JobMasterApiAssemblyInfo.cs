using System.Reflection;

namespace JobMaster.Api.AspNetCore.Internals;

internal static class JobMasterApiAssemblyInfo
{
    internal static Assembly Assembly => typeof(JobMasterApiAssemblyInfo).Assembly;

    internal static string GetServiceId()
    {
        return Assembly.GetCustomAttribute<AssemblyProductAttribute>()?.Product
               ?? Assembly.GetName().Name
               ?? "JobMaster.Api";
    }

    internal static string? GetVersion()
    {
        var version = Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
                      ?? Assembly.GetName().Version?.ToString();

        if (string.IsNullOrWhiteSpace(version))
        {
            return version;
        }

        // Strip build metadata (SemVer: 1.2.3-alpha+commitsha => 1.2.3-alpha)
        var plusIndex = version.IndexOf('+');
        if (plusIndex > 0)
        {
            version = version.Substring(0, plusIndex);
        }

        return version;
    }
}
