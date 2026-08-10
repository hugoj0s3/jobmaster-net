using FluentAssertions;
using JobMaster.Sdk.Abstractions.Serialization;

namespace JobMaster.UnitTests.Sdk.Abstractions.Serialization;

public class InternalJobMasterSerializerTests
{
    [Fact]
    public void Deserialize_Dictionary_WhenValueIsGuid_RecoversAsGuid()
    {
        var guid = Guid.Parse("8e8fd3b4-1c3b-4a2b-9d86-3c28b7c7f7b1");
        var json = InternalJobMasterSerializer.Serialize(new Dictionary<string, object?> { ["id"] = guid });

        var result = InternalJobMasterSerializer.Deserialize<Dictionary<string, object?>>(json);

        result["id"].Should().BeOfType<Guid>();
        result["id"].Should().Be(guid);
    }

    [Fact]
    public void Deserialize_Dictionary_WhenValueIsDateTime_RecoversAsDateTime()
    {
        var dt = new DateTime(2025, 01, 02, 03, 04, 05, DateTimeKind.Utc);
        var json = InternalJobMasterSerializer.Serialize(new Dictionary<string, object?> { ["at"] = dt });

        var result = InternalJobMasterSerializer.Deserialize<Dictionary<string, object?>>(json);

        result["at"].Should().BeOfType<DateTime>();
        result["at"].Should().Be(dt);
    }

    [Fact]
    public void Deserialize_Dictionary_WhenValueHasExplicitTimeZoneOffset_RecoversCorrectAbsoluteInstant()
    {
        // "2025-01-02T03:04:05+02:00" is 2025-01-02T01:04:05Z in UTC. Regression test: RoundtripKind
        // correctly parses an explicit non-Z offset as DateTimeKind.Local (converted to this machine's
        // local time), but a prior version of this code relabeled the result as Utc without converting
        // the ticks, silently shifting the instant by the local machine's own UTC offset.
        var json = "{\"at\":\"2025-01-02T03:04:05+02:00\"}";
        var expectedUtc = new DateTime(2025, 01, 02, 01, 04, 05, DateTimeKind.Utc);

        var result = InternalJobMasterSerializer.Deserialize<Dictionary<string, object?>>(json);

        result["at"].Should().BeOfType<DateTime>();
        ((DateTime)result["at"]!).ToUniversalTime().Should().Be(expectedUtc);
    }

    [Fact]
    public void Deserialize_Dictionary_WhenValueIsOrdinaryString_RemainsString()
    {
        var json = InternalJobMasterSerializer.Serialize(new Dictionary<string, object?> { ["name"] = "hello" });

        var result = InternalJobMasterSerializer.Deserialize<Dictionary<string, object?>>(json);

        result["name"].Should().BeOfType<string>();
        result["name"].Should().Be("hello");
    }

    [Theory]
    [InlineData("short")]
    [InlineData("12345678-1234-1234-1234-12345678901")] // 35 chars: one short of a real Guid
    [InlineData("12345678-1234-1234-1234-1234567890123")] // 37 chars: one over
    [InlineData("zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz")] // right shape, not valid hex
    public void Deserialize_Dictionary_WhenValueLooksLikeButIsNotAGuid_RemainsString(string value)
    {
        var json = InternalJobMasterSerializer.Serialize(new Dictionary<string, object?> { ["v"] = value });

        var result = InternalJobMasterSerializer.Deserialize<Dictionary<string, object?>>(json);

        result["v"].Should().BeOfType<string>();
        result["v"].Should().Be(value);
    }

    [Fact]
    public void Deserialize_Dictionary_WhenValueIsMixedPrimitives_RoundTripsEachCorrectly()
    {
        var guid = Guid.NewGuid();
        var dt = DateTime.UtcNow;
        var values = new Dictionary<string, object?>
        {
            ["str"] = "plain text",
            ["guid"] = guid,
            ["dt"] = dt,
            ["num"] = 42L,
            ["dec"] = 3.14m,
            ["flag"] = true,
        };

        var json = InternalJobMasterSerializer.Serialize(values);
        var result = InternalJobMasterSerializer.Deserialize<Dictionary<string, object?>>(json);

        result["str"].Should().Be("plain text");
        result["guid"].Should().Be(guid);
        result["dt"].Should().Be(dt);
        result["num"].Should().Be(42L);
        result["dec"].Should().Be(3.14m);
        result["flag"].Should().Be(true);
    }
}
