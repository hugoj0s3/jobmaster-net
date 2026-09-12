using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Utils;

namespace JobMaster.UnitTests.Sdk.Abstractions.Config;

/// <summary>
/// Covers the reserved "master fallback" agent connection: it should lazily resolve to a
/// config reusing the cluster's own connection string/repository type, the same way the
/// standalone connection already does.
/// </summary>
public class JobMasterClusterConnectionConfigTests
{
    private static JobMasterClusterConnectionConfig CreateConfig()
    {
        var clusterId = $"c{JobMasterRandomUtil.NewGuid4():N}";
        return JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn");
    }

    [Fact]
    public void TryGetAgentConnectionConfig_ForMasterFallbackName_LazilyCreatesConfigReusingClusterConnection()
    {
        var config = CreateConfig();

        var fallbackConfig = config.TryGetAgentConnectionConfig(JobMasterConstants.MasterFallbackAgentConnName);

        fallbackConfig.Should().NotBeNull();
        fallbackConfig!.ConnectionString.Should().Be(config.ConnectionString);
        fallbackConfig.RepositoryTypeId.Should().Be(config.RepositoryTypeId);
        fallbackConfig.Id.Should().Be($"{config.ClusterId}:{JobMasterConstants.MasterFallbackAgentConnName}");
    }

    [Fact]
    public void TryGetAgentConnectionConfig_ForMasterFallbackName_CalledTwice_ReturnsSameRegisteredConfig()
    {
        var config = CreateConfig();

        var first = config.TryGetAgentConnectionConfig(JobMasterConstants.MasterFallbackAgentConnName);
        var second = config.TryGetAgentConnectionConfig(JobMasterConstants.MasterFallbackAgentConnName);

        second.Should().BeSameAs(first);
        config.GetAllAgentConnectionConfigs().Should().ContainSingle(c => c.Id == first!.Id);
    }

    [Fact]
    public void GetAgentConnectionConfig_ForMasterFallbackName_ResolvesWithoutExplicitRegistration()
    {
        var config = CreateConfig();

        var fallbackConfig = config.GetAgentConnectionConfig(JobMasterConstants.MasterFallbackAgentConnName);

        fallbackConfig.Should().NotBeNull();
    }

    // ── IsPriorityDisabled / SetDisabledPriorities ────────────────────────────

    [Fact]
    public void IsPriorityDisabled_WhenNothingSet_ReturnsFalseForAllPriorities()
    {
        var config = CreateConfig();

        foreach (JobMasterPriority p in Enum.GetValues(typeof(JobMasterPriority)))
            config.IsPriorityDisabled(p).Should().BeFalse($"{p} should not be disabled by default");
    }

    [Theory]
    [InlineData(JobMasterPriority.VeryLow)]
    [InlineData(JobMasterPriority.Low)]
    [InlineData(JobMasterPriority.High)]
    [InlineData(JobMasterPriority.Critical)]
    public void IsPriorityDisabled_AfterSetDisabledPriorities_ReturnsTrueForDisabledPriority(JobMasterPriority priority)
    {
        var config = CreateConfig();
        config.SetDisabledPriorities(new HashSet<JobMasterPriority> { priority });

        config.IsPriorityDisabled(priority).Should().BeTrue();
    }

    [Fact]
    public void IsPriorityDisabled_AfterSetDisabledPriorities_ReturnsFalseForNonDisabledPriority()
    {
        var config = CreateConfig();
        config.SetDisabledPriorities(new HashSet<JobMasterPriority> { JobMasterPriority.VeryLow });

        config.IsPriorityDisabled(JobMasterPriority.High).Should().BeFalse();
        config.IsPriorityDisabled(JobMasterPriority.Critical).Should().BeFalse();
        config.IsPriorityDisabled(JobMasterPriority.Medium).Should().BeFalse();
    }

    [Fact]
    public void IsPriorityDisabled_SetDisabledPriorities_IsAdditive()
    {
        var config = CreateConfig();
        config.SetDisabledPriorities(new HashSet<JobMasterPriority> { JobMasterPriority.VeryLow });
        config.SetDisabledPriorities(new HashSet<JobMasterPriority> { JobMasterPriority.Critical });

        config.IsPriorityDisabled(JobMasterPriority.VeryLow).Should().BeTrue();
        config.IsPriorityDisabled(JobMasterPriority.Critical).Should().BeTrue();
    }
}
