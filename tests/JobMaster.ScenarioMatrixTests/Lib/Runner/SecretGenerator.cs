using System.Security.Cryptography;

namespace JobMaster.ScenarioMatrixTests.Lib;

internal static class SecretGenerator
{
    public static string Generate(int byteLength = 18)
    {
        var bytes = RandomNumberGenerator.GetBytes(byteLength);
        return Convert.ToBase64String(bytes).Replace("+", "-").Replace("/", "_").TrimEnd('=');
    }
}
