using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Abstractions.Ioc.Definitions;

internal sealed class AgentConnectionDefinition
{
    public AgentConnectionDefinition()
    {
        // Single provider by default: one provider serves both roles.
        var single = new AgentProviderDefinition();
        Transport = single;
        Execution = single;
    }

    public string ClusterId { get; set; } = string.Empty;
    public string AgentConnectionName { get; set; } = string.Empty;
    public bool ProtectConnectionChanges { get; set; } = true;

    /// <summary>Same instance as <see cref="Execution"/> for a single-provider connection.</summary>
    public AgentProviderDefinition Transport { get; private set; }
    /// <summary>Same instance as <see cref="Transport"/> for a single-provider connection.</summary>
    public AgentProviderDefinition Execution { get; private set; }

    public bool IsHybrid => !ReferenceEquals(Transport, Execution);

    public string? AgentRepoType => IsHybrid ? HybridRepoTypeOrNull() : SingleRepoTypeOrNull();
    public string? AgentConnString => IsHybrid ? null : Transport.AgentConnString;
    public JobMasterConfigDictionary? AgentAdditionalConnConfig => IsHybrid ? null : Transport.AgentAdditionalConnConfig;

    /// <summary>Switches to separate transport and execution providers. No-op when already hybrid.</summary>
    public void UseHybrid()
    {
        if (IsHybrid)
            return;

        if (!string.IsNullOrEmpty(Transport.AgentRepoType) ||
            !string.IsNullOrEmpty(Transport.AgentConnString) ||
            Transport.AgentAdditionalConnConfig != null)
        {
            throw new InvalidOperationException("Cannot configure both a single provider and UseHybrid() on the same agent connection.");
        }

        Transport = new AgentProviderDefinition();
        Execution = new AgentProviderDefinition();
    }

    public void Validate()
    {
        var (isValid, error) = IsValid();
        if (!isValid)
        {
            throw new ArgumentException(error);
        }
    }

    public (bool IsValid, string? Error) IsValid()
    {
        if (string.IsNullOrWhiteSpace(AgentConnectionName))
        {
            return (false, "AgentConnectionName must be defined via AgentConnectionName(...)");
        }

        if (IsHybrid)
        {
            var transportResult = IsValidLeg(Transport, "Transport");
            if (!transportResult.IsValid) return transportResult;
            
            return IsValidLeg(Execution, "Execution");
        }

        if (string.IsNullOrWhiteSpace(AgentConnString))
        {
            return (false, "AgentConnString must be defined via AgentConnString(...)");
        }

        if (string.IsNullOrWhiteSpace(AgentRepoType))
        {
            return (false, "AgentRepoType must be defined via AgentRepoType(...)");
        }

        // Hybrid connections are configured via UseHybrid()/Transport/Execution, never as a raw "hybrid:..." repo type.
        if (!RepoTypeAndFingerprintUtil.IsValidSingleRepoTypeId(AgentRepoType))
        {
            return (false, RepoTypeAndFingerprintUtil.InvalidSingleRepoTypeIdMessage(AgentRepoType));
        }
        
        return (true, null);
    }

    // Concrete provider repo types: the single provider, or the transport and execution legs. Call after Validate().
    public IEnumerable<string> GetProviderRepoTypes() =>
        new[] { Transport, Execution }.Distinct().Select(t => t.AgentRepoType!);

    // Stored as-is (no validation), so IsValid() can report an invalid value instead of "must be defined".
    private string? SingleRepoTypeOrNull() => Transport.AgentRepoType;

    // "hybrid:<t>&&<e>" once both legs have a valid provider repo type; null until then (IsValid() reports why).
    private string? HybridRepoTypeOrNull() =>
        RepoTypeAndFingerprintUtil.IsValidSingleRepoTypeId(Transport.AgentRepoType) &&
        RepoTypeAndFingerprintUtil.IsValidSingleRepoTypeId(Execution.AgentRepoType)
            ? RepoTypeAndFingerprintUtil.FormatHybridRepoType(Transport.AgentRepoType!, Execution.AgentRepoType!)
            : null;

    private (bool IsValid, string? Error) IsValidLeg(AgentProviderDefinition leg, string role)
    {
        if (string.IsNullOrWhiteSpace(leg.AgentConnString) ||
            string.IsNullOrWhiteSpace(leg.AgentRepoType))
        {
            return (false, $"Hybrid agent connection '{AgentConnectionName}' must define both {role} repoType and connectionString.");
        }

        // Each leg must be a concrete provider; a hybrid leg (nested hybrid) is rejected here.
        if (!RepoTypeAndFingerprintUtil.IsValidSingleRepoTypeId(leg.AgentRepoType))
        {
            return (false, RepoTypeAndFingerprintUtil.InvalidSingleRepoTypeIdMessage(leg.AgentRepoType));
        }
        
        return (true, null);
    }
}
