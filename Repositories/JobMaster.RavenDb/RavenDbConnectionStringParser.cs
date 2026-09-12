namespace JobMaster.RavenDb;

/// <summary>
/// RavenDB has no native ADO-style connection string (the client takes <c>Urls[]</c> + <c>Database</c>
/// directly), so this is our own convention: semicolon-separated "Key=Value1,Value2" segments, e.g.
/// "Urls=http://node1:8080,http://node2:8080;Database=MyDb".
/// </summary>
internal static class RavenDbConnectionStringParser
{
    private const string UrlsKey = "Urls";
    private const string DatabaseKey = "Database";

    public static (string[] Urls, string Database) Parse(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("RavenDB connection string must not be empty.", nameof(connectionString));
        }

        var urls = GetValues(connectionString, UrlsKey);
        if (urls.Length == 0)
        {
            throw new ArgumentException(
                $"RavenDB connection string must include a '{UrlsKey}=...' segment, e.g. \"Urls=http://localhost:8080;Database=MyDb\".",
                nameof(connectionString));
        }

        var databaseValues = GetValues(connectionString, DatabaseKey);
        if (databaseValues.Length != 1)
        {
            throw new ArgumentException(
                $"RavenDB connection string must include exactly one '{DatabaseKey}=<name>' segment, e.g. \"Urls=http://localhost:8080;Database=MyDb\".",
                nameof(connectionString));
        }

        return (urls, databaseValues[0]);
    }

    /// <summary>
    /// Finds the "key=value1,value2,..." segment matching <paramref name="key"/> (case-insensitive) and
    /// returns its comma-split, trimmed values. Empty array if the key isn't present.
    /// </summary>
    private static string[] GetValues(string connectionString, string key)
    {
        // Char-param Split(char, StringSplitOptions) is unavailable on netstandard2.0 -- use the char[] overload.
        var segments = connectionString.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
        foreach (var segment in segments)
        {
            var parts = segment.Split(new[] { '=' }, 2);
            if (parts.Length != 2 || !string.Equals(parts[0].Trim(), key, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            return parts[1]
                .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(v => v.Trim())
                .Where(v => v.Length > 0)
                .ToArray();
        }

        return Array.Empty<string>();
    }
}
