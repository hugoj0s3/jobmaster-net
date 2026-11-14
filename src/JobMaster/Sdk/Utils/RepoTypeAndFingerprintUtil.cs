using JobMaster.Sdk.Reflection;

namespace JobMaster.Sdk.Utils;

internal static class RepoTypeAndFingerprintUtil
{
    private const string Prefix = "hybrid:";
    private const string Separator = "&&";
    private const int MaxNonHybridRepoTypeLength = 50;
    private const int MaxNonHybridFingerprintLength = 100;

    public static string FormatHybridRepoType(string transportRepoType, string executionRepoType)
    {
        ValidateSingleRepoTypeId(transportRepoType);
        ValidateSingleRepoTypeId(executionRepoType);

        return $"{Prefix}{ProviderAssemblies.ToKnownRepositoryType(transportRepoType)}{Separator}{ProviderAssemblies.ToKnownRepositoryType(executionRepoType)}";
    }
    
    public static string FormatHybridFingerprint(string transportFingerprint, string executionFingerprint)
    {
        ValidateSingleFingerprint(transportFingerprint);
        ValidateSingleFingerprint(executionFingerprint);

        return $"{Prefix}{transportFingerprint}{Separator}{executionFingerprint}";
    }

    public static bool IsHybrid(string? value) =>
        !string.IsNullOrEmpty(value) &&
        value!.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase);

    // Null when not a hybrid repo type or either leg is invalid.
    public static (string Transport, string Execution)? TryParseHybridRepoType(string? repoType)
    {
        if (!IsHybrid(repoType))
            return null;

        var (transport, execution) = SplitHybrid(repoType);
        if (!IsValidSingleRepoTypeId(transport) || !IsValidSingleRepoTypeId(execution))
            return null;

        return (transport, execution);
    }

    // Null when not a hybrid fingerprint or either leg is invalid.
    public static (string Transport, string Execution)? TryParseHybridFingerprint(string? fingerprint)
    {
        if (!IsHybrid(fingerprint))
            return null;

        var (transport, execution) = SplitHybrid(fingerprint);
        if (!IsValidSingleFingerprint(transport) || !IsValidSingleFingerprint(execution))
            return null;

        return (transport, execution);
    }

    public static bool IsValidRepoTypeId(string? repoType)
    {
        if (string.IsNullOrWhiteSpace(repoType))
            return false;

        if (IsHybrid(repoType))
        {
            return TryParseHybridRepoType(repoType) != null;
        }

        return IsValidSingleRepoTypeId(repoType);
    }

    public static void ValidateSingleRepoTypeId(string repoType)
    {
        if (!IsValidSingleRepoTypeId(repoType))
        {
            throw new InvalidOperationException(InvalidSingleRepoTypeIdMessage(repoType));
        }
    }

    public static string InvalidSingleRepoTypeIdMessage(string? repoType) =>
        $"Invalid non-hybrid RepositoryType '{repoType}'. Only a-z, A-Z, 0-9, '-', and '_' are allowed (max {MaxNonHybridRepoTypeLength} chars).";

    public static bool IsValidFingerprint(string? fingerprint)
    {
        if (string.IsNullOrWhiteSpace(fingerprint))
            return false;

        if (IsHybrid(fingerprint))
        {
            return TryParseHybridFingerprint(fingerprint) != null;
        }

        return IsValidSingleFingerprint(fingerprint);
    }

    public static bool IsValid(string repoType, string fingerprint)
    {
        if (IsHybrid(repoType) != IsHybrid(fingerprint))
        {
            return false;
        }
        
        return IsValidFingerprint(fingerprint) && IsValidRepoTypeId(repoType);
    }

    public static void ValidateFingerprint(string fingerprint)
    {
        if (!IsValidFingerprint(fingerprint))
        {
            throw new InvalidOperationException($"Invalid agent fingerprint '{fingerprint}'.");
        }
    }

    public static void ValidateSingleFingerprint(string fingerprint)
    {
        if (!IsValidSingleFingerprint(fingerprint))
        {
            throw new InvalidOperationException(
                $"Invalid non-hybrid agent fingerprint '{fingerprint}'. ':' and '{Separator}' are not allowed (max {MaxNonHybridFingerprintLength} chars).");
        }
    }
    
    public static string GenerateSingleFingerprint() => JobMasterRandomUtil.NewGuid4().ToString("N");

    public static bool IsValidSingleRepoTypeId(string? repoType)
    {
        if (string.IsNullOrWhiteSpace(repoType) ||
            repoType!.Length > MaxNonHybridRepoTypeLength ||
            IsHybrid(repoType))
        {
            return false;
        }

        return repoType.All(c => c is >= 'a' and <= 'z' or >= 'A' and <= 'Z' or >= '0' and <= '9' or '-' or '_');
    }

    private static bool IsValidSingleFingerprint(string? fingerprint)
    {
        if (string.IsNullOrWhiteSpace(fingerprint) ||
            fingerprint!.Length > MaxNonHybridFingerprintLength)
        {
            return false;
        }

        return fingerprint.IndexOf(':') < 0 &&
               fingerprint.IndexOf(Separator, StringComparison.Ordinal) < 0;
    }
    
    private static (string TransportValue, string ExecutionValue) SplitHybrid(string? value)
    {
        if (value is null)
        {
            return (string.Empty, string.Empty);
        }
        
        if (!IsHybrid(value))
            return (string.Empty, string.Empty);
        
        var body = value.Substring(Prefix.Length);
        var sepIndex = body.IndexOf(Separator, StringComparison.Ordinal);
        if (sepIndex <= 0)
            return (string.Empty, string.Empty);
        
        return (body.Substring(0, sepIndex), body.Substring(sepIndex + Separator.Length));
    }
}
