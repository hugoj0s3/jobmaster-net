using System.Security.Cryptography;

namespace JobMaster.Benchmarks.Common.Containers;

/// <summary>Mirrors Tests/JobMaster.ScenarioTests/Runner/SecretGenerator.cs -- generates a random
/// secret instead of a hardcoded literal. Benchmark containers are ephemeral and only ever reachable
/// on the local Docker network for the lifetime of one run, but a fixed password is still worth
/// avoiding on general security-hygiene grounds.</summary>
public static class SecretGenerator
{
    public static string Generate(int byteLength = 18)
    {
        var bytes = RandomNumberGenerator.GetBytes(byteLength);
        return Convert.ToBase64String(bytes).Replace("+", "-").Replace("/", "_").TrimEnd('=');
    }
}
