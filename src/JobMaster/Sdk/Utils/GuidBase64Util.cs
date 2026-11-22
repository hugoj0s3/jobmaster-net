namespace JobMaster.Sdk.Utils;

internal static class GuidBase64Util
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
    public static Guid ParseFlexible(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException("Value cannot be null or empty.", nameof(value));

        if (Guid.TryParse(value, out var guid))
            return guid;

        return FromBase64(value);
    }

    /// <summary>
    /// The GUID in <paramref name="value"/>, in either format <see cref="ParseFlexible"/> reads (the compact form as
    /// <see cref="ToBase64"/> writes it, 22 characters), or null when it isn't one.
    /// </summary>
    public static Guid? TryParseFlexible(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        if (Guid.TryParse(value, out var guid))
            return guid;

        return IsBase64UrlGuid(value!) ? FromBase64(value!) : null;
    }

    public static IList<string> ToGuidsForms(string? value)
    {
        var result = new List<string>();

        if (string.IsNullOrWhiteSpace(value))
            return result;

        var guidId = TryParseFlexible(value);
        if (guidId is null)
        {
            return result;
        }
        
        result.Add(guidId.Value.ToString());
        result.Add(guidId.Value.ToString("N"));
        result.Add(guidId.Value.ToBase64());

        return result;
    }

    // Exactly what ToBase64 writes: 22 base64url characters, which FromBase64 always decodes to 16 bytes.
    private static bool IsBase64UrlGuid(string value)
    {
        if (value.Length != 22)
            return false;

        foreach (var c in value)
        {
            if (!(c is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or >= '0' and <= '9' or '-' or '_'))
                return false;
        }

        return true;
    }

    private static Guid FromBase64(string base64)
    {
        if (string.IsNullOrWhiteSpace(base64))
            throw new ArgumentException("Value cannot be null or empty.", nameof(base64));

        var b64 = base64.Replace('-', '+').Replace('_', '/');

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
