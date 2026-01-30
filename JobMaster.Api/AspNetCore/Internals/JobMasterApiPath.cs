namespace JobMaster.Api.AspNetCore.Internals;

internal static class JobMasterApiPath
{
    internal static string NormalizeBasePath(string? basePath)
    {
        if (string.IsNullOrWhiteSpace(basePath))
        {
            return "/jm-api";
        }

        basePath = basePath.Trim();

        if (!basePath.StartsWith('/'))
        {
            basePath = "/" + basePath;
        }

        if (basePath.Length > 1 && basePath.EndsWith('/'))
        {
            basePath = basePath.TrimEnd('/');
        }

        return basePath;
    }
}
