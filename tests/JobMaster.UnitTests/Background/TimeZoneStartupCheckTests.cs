using FluentAssertions;
using JobMaster.Sdk.Background;

namespace JobMaster.UnitTests.Background;

public class TimeZoneStartupCheckTests
{
    [Fact]
    public void ExplicitZoneDifferentFromLocal_OnRestart_ShouldPass()
    {
        // First start saved America/Sao_Paulo; restarting on a Lisbon machine with the same setting must not fail.
        JobMasterRuntime.ValidateOmittedTimeZone("c1", "America/Sao_Paulo", "America/Sao_Paulo", "Europe/Lisbon")
            .Should().BeNull();
    }

    [Fact]
    public void NotSet_StoredZoneDiffersFromLocal_ShouldAskToSetIt()
    {
        JobMasterRuntime.ValidateOmittedTimeZone("c1", null, "UTC", "Europe/Lisbon")
            .Should().Contain("must set IanaTimeZoneId explicitly").And.Contain("Europe/Lisbon").And.Contain("IanaTimeZoneId(\"UTC\")");
    }

    [Fact]
    public void NotSet_StoredZoneMatchesLocal_ShouldPass()
    {
        JobMasterRuntime.ValidateOmittedTimeZone("c1", null, "UTC", "UTC").Should().BeNull();
    }
}
