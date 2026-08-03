namespace JobMaster.Benchmarks.Common.Containers;

internal static class RepoRootLocator
{
    public static string Find()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !File.Exists(Path.Combine(dir.FullName, "JobMaster.sln")))
        {
            dir = dir.Parent;
        }

        return dir?.FullName
            ?? throw new InvalidOperationException("Could not locate repo root (JobMaster.sln) from " + AppContext.BaseDirectory);
    }
}
