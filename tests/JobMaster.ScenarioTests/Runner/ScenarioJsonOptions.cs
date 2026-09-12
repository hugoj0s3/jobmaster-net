using System.Text.Json;

namespace JobMaster.ScenarioTests.Runner;

internal static class ScenarioJsonOptions
{
    public static readonly JsonSerializerOptions Default = new()
    {
        PropertyNameCaseInsensitive = true
    };
}
