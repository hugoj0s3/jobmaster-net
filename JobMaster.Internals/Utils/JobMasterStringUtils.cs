using System.Text;

#if JOBMASTER
namespace JobMaster.Internals;
#elif NATJS
namespace JobMaster.NatsJetStream.Internals.Utils;
#elif SQLPROV
namespace JobMaster.SqlBase.Internals.Utils;
#else
namespace JobMaster.Internal.Utils;
#endif

internal static class JobMasterStringUtils
{
    /// <summary>
    /// Validates that a string is valid for use as an ID in JobMaster system
    /// Only allows letters, numbers, underscore, hyphen, and dot
    /// </summary>
    /// <param name="value">The string value to validate</param>
    /// <param name="acceptColon"></param>
    /// <returns>True if the string format is valid for ID use, false otherwise</returns>
    public static bool IsValidForId(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return false;
        
        if (value.Length > 255) 
            return false;
        
        if (value.Any(c => !char.IsLetterOrDigit(c) && c != '_' && c != '-' && c != '.' && c != ':'))
            return false;
        
        // valid double separators
        if (value.Contains("..") || value.Contains("::") || value.Contains(".:") || value.Contains(":."))
        {
            return false;
        }
        
        var segments = value.Split('.', ':');
        
        return segments.All(x => IsValidForSegment(x));
    }

    public static bool IsValidForSegment(string value, int maxLen = 50)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        if (value.Length > maxLen)
        {
            return false;
        }

        // valid double hyphens/underscores
        if (value.Contains("--") || value.Contains("__") || value.Contains("-_") || value.Contains("_-"))
        {
            return false;
        }

        return value.All(c => char.IsLetterOrDigit(c) || c == '_' || c == '-');
    }
    
    public static string SanitizeForName(string value, int maxLen = 50)
    {
        if (string.IsNullOrWhiteSpace(value)) return "x";

        // Allow only letters, digits, '_' and '-'
        var sb = new StringBuilder(value.Length);
        foreach (var c in value)
        {
            if (char.IsLetterOrDigit(c) || c == '_' || c == '-') sb.Append(c);
            // map common separators/spaces to '-'
            else if (char.IsWhiteSpace(c) || c == '.' || c == ':' || c == '/' || c == '\\') sb.Append('-');
            // else drop
        }

        var s = sb.ToString();

        // Collapse double hyphens/underscores
        while (s.Contains("--")) s = s.Replace("--", "-");
        while (s.Contains("__")) s = s.Replace("__", "_");

        // Trim edge separators
        s = s.Trim('-', '_');

        // Fallback if empty
        if (s.Length == 0) s = "x";

        // Enforce max length
        if (s.Length > maxLen) s = s.Substring(0, maxLen);

        return s;
    }
}
