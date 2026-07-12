using System.Text.Json;
using System.Text.Json.Serialization;

namespace JobMaster.ScenarioTests.Runner;

public sealed class ContainerDefinition
{
    public string ContainerName { get; set; } = "";
    public string DockerfilePath { get; set; } = "";
    public string NetworkAlias { get; set; } = "";
    public int HttpPort { get; set; } = 8080;
    public string HealthCheckPath { get; set; } = "/health";
    public AuthDefinition? Auth { get; set; }

    /// <summary>Only meaningful for the api container. Defaults to "/jm-api" when unset.</summary>
    public string? ApiBasePath { get; set; }

    [JsonPropertyName("clusterConfigTemplates")]
    public List<JsonElement> ClusterConfigTemplates { get; set; } = new();
}

/// <summary>
/// Any combination of ApiKey / Username+Password can be set at once — mirrors JobMasterApiOptions
/// supporting multiple simultaneous auth mechanisms, so one scenario can exercise several.
/// Values are usually {{ApiKey}}/{{ApiUsername}}/{{ApiPassword}} tokens filled in by ScenarioRunner
/// with freshly generated secrets, not literals authored in the JSON.
/// </summary>
public sealed class AuthDefinition
{
    public bool RequireAuthentication { get; set; }
    public string? ApiKey { get; set; }
    public string? Username { get; set; }
    public string? Password { get; set; }

    /// <summary>
    /// Enables JWT bearer auth. No secret is templated in here — the container generates its own
    /// signing key at startup and exposes a `/auth/token` endpoint a test can call to mint a valid
    /// token, so the signing key never needs to leave the container.
    /// </summary>
    public bool EnableJwt { get; set; }
}
