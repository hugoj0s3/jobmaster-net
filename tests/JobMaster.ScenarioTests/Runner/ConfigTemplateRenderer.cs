namespace JobMaster.ScenarioTests.Runner;

internal static class ConfigTemplateRenderer
{
    public static string Render(string rawJson, IReadOnlyDictionary<string, string> tokens)
    {
        var rendered = rawJson;
        foreach (var (key, value) in tokens)
        {
            rendered = rendered.Replace("{{" + key + "}}", value, StringComparison.Ordinal);
        }

        return rendered;
    }
}
