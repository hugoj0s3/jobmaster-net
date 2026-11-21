using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace JobMaster.ScenarioMatrixTests.Flavors;

/// <summary>
/// Fills a scenario JSON template for one flavor. The template stays valid JSON:
/// <list type="bullet">
/// <item><c>{{flavor}}</c>: the flavor's code (container names, aliases, cluster ids).</item>
/// <item><c>{{master:RepoType}}</c>, <c>{{master:ConnectionString:&lt;db&gt;}}</c>: the master provider, with a
/// database name unique to the flavor.</item>
/// <item><c>"Connection": "{{agent:N}}"</c> inside an agent connection: replaced by the flavor's fields for slot N
/// (single provider, or <c>Transport</c> + <c>Execution</c>); the other properties, such as <c>Name</c>, stay.</item>
/// </list>
/// The runner's infrastructure tokens (<c>{{PostgresHost}}</c>, ...) are left for it to render.
/// </summary>
public static class FlavorTemplateRenderer
{
    private const string AgentProperty = "Connection";

    private static readonly Regex MasterConnectionStringToken = new(@"\{\{master:ConnectionString:([A-Za-z0-9]+)\}\}", RegexOptions.Compiled);
    private static readonly Regex AgentToken = new(@"^\{\{agent:([1-9][0-9]*)\}\}$", RegexOptions.Compiled);

    private static readonly JsonSerializerOptions WriteOptions = new()
    {
        WriteIndented = true,
        // Keep connection strings readable (no & for '&' etc.); the result is only read by the runner.
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    public static string Render(string template, ScenarioFlavor flavor)
    {
        var text = template
            .Replace("{{flavor}}", flavor.Code, StringComparison.Ordinal)
            .Replace("{{master:RepoType}}", flavor.MasterRepoType, StringComparison.Ordinal);
        text = MasterConnectionStringToken.Replace(text, m => flavor.MasterConnectionString(m.Groups[1].Value));

        var root = JsonNode.Parse(text) ?? throw new InvalidOperationException("Empty scenario template.");
        ExpandAgents(root, flavor);
        return root.ToJsonString(WriteOptions);
    }

    private static void ExpandAgents(JsonNode node, ScenarioFlavor flavor)
    {
        if (node is JsonArray array)
        {
            foreach (var item in array)
            {
                if (item != null)
                {
                    ExpandAgents(item, flavor);
                }
            }

            return;
        }

        if (node is not JsonObject obj)
        {
            return;
        }

        if (obj[AgentProperty] is JsonValue value && value.TryGetValue<string>(out var token))
        {
            var match = AgentToken.Match(token);
            if (!match.Success)
            {
                throw new InvalidOperationException($"\"{AgentProperty}\" must be an {{{{agent:N}}}} placeholder, got \"{token}\".");
            }

            obj.Remove(AgentProperty);
            foreach (var (key, fieldValue) in flavor.Agent(int.Parse(match.Groups[1].Value)))
            {
                obj[key] = fieldValue?.DeepClone();
            }

            return;
        }

        foreach (var (_, child) in obj.ToList())
        {
            if (child != null)
            {
                ExpandAgents(child, flavor);
            }
        }
    }
}
