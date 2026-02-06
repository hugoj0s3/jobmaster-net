using Microsoft.Extensions.Configuration;
using System.Text.RegularExpressions;

namespace JobMaster.IntegrationTests.Utils;

internal static class IntegrationTestSecrets
{
    internal static string ApplySecrets(string connectionString, string dbProvider, IConfiguration config)
    {
        return ApplySecrets(connectionString, config);
    }

    internal static string ApplySecrets(string connectionString, IConfiguration config)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return connectionString;
        }

        return ReplaceTokens(connectionString, config);
    }

    private static string ReplaceTokens(string value, IConfiguration config)
    {
        var replaced = value;

        var secretTokenMatches = Regex.Matches(value, @"\bsecret_[A-Za-z0-9_]+\b");
        foreach (Match match in secretTokenMatches)
        {
            var token = match.Value;
            if (string.IsNullOrWhiteSpace(token))
            {
                continue;
            }

            var secretValue = config[token];
            if (string.IsNullOrWhiteSpace(secretValue))
            {
                continue;
            }

            replaced = replaced.Replace(token, secretValue, StringComparison.Ordinal);
        }

        var bracketTokenMatches = Regex.Matches(value, @"\[[A-Za-z0-9_]+\]");
        foreach (Match match in bracketTokenMatches)
        {
            var token = match.Value;
            if (string.IsNullOrWhiteSpace(token) || token.Length < 3)
            {
                continue;
            }

            var key = token.Substring(1, token.Length - 2);
            if (string.IsNullOrWhiteSpace(key))
            {
                continue;
            }

            var secretValue = config[key];
            if (string.IsNullOrWhiteSpace(secretValue))
            {
                continue;
            }

            replaced = replaced.Replace(token, secretValue, StringComparison.Ordinal);
        }

        return replaced;
    }
}
