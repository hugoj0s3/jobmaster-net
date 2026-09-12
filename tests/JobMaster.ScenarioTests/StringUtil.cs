using System.Text;

namespace JobMaster.ScenarioTests;

public static class StringUtil
{
    public static string ToKebabCase(this string str)
    {
        if (string.IsNullOrEmpty(str))
        {
            return string.Empty;
        }
        
        var result = new StringBuilder();
        result.Append(char.ToLower(str[0]));
        
        foreach (var c in str.Substring(1))
        {
            if (Char.IsUpper(c))
            {
                result.Append("-");
                result.Append(Char.ToLower(c));
            }
            else
            {
                result.Append(c);
            }
        }
        
        return result.ToString();
    }

    /// <summary>Inverse of ToKebabCase: "phase-1" -> "Phase1", "basic-execution" -> "BasicExecution".</summary>
    public static string ToPascalCase(this string str)
    {
        if (string.IsNullOrEmpty(str))
        {
            return string.Empty;
        }

        var result = new StringBuilder();
        foreach (var part in str.Split('-', StringSplitOptions.RemoveEmptyEntries))
        {
            result.Append(char.ToUpper(part[0]));
            if (part.Length > 1)
            {
                result.Append(part.Substring(1));
            }
        }

        return result.ToString();
    }
}