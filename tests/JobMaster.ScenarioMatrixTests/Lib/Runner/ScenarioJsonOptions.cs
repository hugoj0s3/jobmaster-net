using System.Text.Json;

namespace JobMaster.ScenarioMatrixTests.Lib;

internal static class ScenarioJsonOptions
{
    public static readonly JsonSerializerOptions Default = new()
    {
        PropertyNameCaseInsensitive = true
    };
}
