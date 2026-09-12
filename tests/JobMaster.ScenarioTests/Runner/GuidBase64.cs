namespace JobMaster.ScenarioTests.Runner;

/// <summary>
/// Mirrors JobMaster.Api's internal GuidBase64Extensions encoding (base64url, unpadded) so this
/// project can match ApiJobModel.Id values back to the Guids it scheduled, without taking a
/// JobMaster reference just to share the algorithm.
/// </summary>
internal static class GuidBase64
{
    public static Guid Parse(string value)
    {
        if (Guid.TryParse(value, out var guid))
        {
            return guid;
        }

        var b64 = value.Replace('-', '+').Replace('_', '/');
        b64 = (b64.Length % 4) switch
        {
            0 => b64,
            2 => b64 + "==",
            3 => b64 + "=",
            _ => throw new FormatException($"Invalid base64url GUID '{value}'.")
        };

        return new Guid(Convert.FromBase64String(b64));
    }
}
