using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioMatrixTests.Flavors;

/// <summary>
/// The stop types a scenario that stops an app runs with, chosen like flavors: every type unless excluded with a
/// reason. The scenario combines them with its flavors into its theory data.
/// </summary>
public static class ScenarioStopTypes
{
    public static readonly IReadOnlyList<ScenarioStopType> All = [ScenarioStopType.SigTerm, ScenarioStopType.Crash];

    /// <summary>Every stop type except <paramref name="excluded"/>; each exclusion needs a reason.</summary>
    public static IReadOnlyList<ScenarioStopType> AllExcept(params (ScenarioStopType StopType, string Reason)[] excluded)
    {
        foreach (var (stopType, reason) in excluded)
        {
            if (string.IsNullOrWhiteSpace(reason))
            {
                throw new ArgumentException($"Excluding stop type '{stopType}' needs a reason.", nameof(excluded));
            }
        }

        var result = All.Where(t => excluded.All(e => e.StopType != t)).ToList();
        if (result.Count == 0)
        {
            throw new ArgumentException("A scenario that stops an app needs at least one stop type.", nameof(excluded));
        }

        return result;
    }
}
