namespace JobMaster.Sdk.Abstractions.Background;

/// <summary>
/// A worker's running runners. Runners add and remove themselves from their own tasks while stops read the list, so
/// every access is locked and reads get a copy.
/// </summary>
internal sealed class RunnerRegistry
{
    private readonly object sync = new();
    private readonly List<IJobMasterRunner> runners = new();

    public void Add(IJobMasterRunner runner)
    {
        lock (sync)
        {
            runners.Add(runner);
        }
    }

    public void Remove(IJobMasterRunner runner)
    {
        lock (sync)
        {
            runners.Remove(runner);
        }
    }

    public IReadOnlyList<IJobMasterRunner> Snapshot()
    {
        lock (sync)
        {
            return runners.ToList();
        }
    }
}
