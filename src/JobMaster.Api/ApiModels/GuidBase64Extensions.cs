namespace JobMaster.Api.ApiModels;

internal static class GuidBase64Extensions
{
    public static string ToBase64(this Guid guid)
    {
        var bytes = guid.ToByteArray(); // 16 bytes
        var b64 = Convert.ToBase64String(bytes); // 24 chars with "=="
        return b64.TrimEnd('=').Replace('+', '-').Replace('/', '_'); // 22 chars
    }

    /// <summary>
    /// Parses a GUID from either standard format (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
    /// or base64url-encoded format (22-character compact form).
    /// </summary>
    public static Guid ParseFlexible(this string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException("Value cannot be null or empty.", nameof(value));

        if (Guid.TryParse(value, out var guid))
            return guid;

        return value.FromBase64();
    }
    
    private static Guid FromBase64(this string base64Url)
    {
        if (string.IsNullOrWhiteSpace(base64Url))
            throw new ArgumentException("Value cannot be null or empty.", nameof(base64Url));

        var b64 = base64Url.Replace('-', '+').Replace('_', '/');

        // restore padding
        switch (b64.Length % 4)
        {
            case 0: break;
            case 2: b64 += "=="; break;
            case 3: b64 += "="; break;
            default:
                throw new FormatException("Invalid base64url string length.");
        }

        var bytes = Convert.FromBase64String(b64);
        if (bytes.Length != 16)
            throw new FormatException("Invalid GUID base64url payload length.");

        return new Guid(bytes);
    }
}