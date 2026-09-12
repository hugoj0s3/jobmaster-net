using System.Security.Cryptography;

namespace JobMaster.ScenarioTests.Runner;

internal static class SecretGenerator
{
    public static string Generate(int byteLength = 18)
    {
        var bytes = RandomNumberGenerator.GetBytes(byteLength);
        return Convert.ToBase64String(bytes).Replace("+", "-").Replace("/", "_").TrimEnd('=');
    }
}
