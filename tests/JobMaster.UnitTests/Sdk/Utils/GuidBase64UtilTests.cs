using FluentAssertions;
using JobMaster.Sdk.Utils;

namespace JobMaster.UnitTests.Sdk.Utils;

public class GuidBase64UtilTests
{
    [Fact]
    public void ParseFlexible_WithStandardGuidFormat_ReturnsGuid()
    {
        var guid = Guid.NewGuid();
        GuidBase64Util.ParseFlexible(guid.ToString("D")).Should().Be(guid);
    }

    [Fact]
    public void ParseFlexible_WithBase64UrlFormat_ReturnsGuid()
    {
        var guid = Guid.NewGuid();
        GuidBase64Util.ParseFlexible(guid.ToBase64()).Should().Be(guid);
    }

    [Fact]
    public void ParseFlexible_WithNoDashesFormat_ReturnsGuid()
    {
        var guid = Guid.NewGuid();
        GuidBase64Util.ParseFlexible(guid.ToString("N")).Should().Be(guid);
    }

    [Fact]
    public void ParseFlexible_RoundTrip_Succeeds()
    {
        var guid = Guid.NewGuid();
        GuidBase64Util.ParseFlexible(guid.ToBase64()).Should().Be(guid);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void ParseFlexible_WhenNullOrWhitespace_ThrowsArgumentException(string value)
    {
        var act = () => GuidBase64Util.ParseFlexible(value);
        act.Should().Throw<ArgumentException>();
    }

    [Theory]
    [InlineData("not-a-guid")]
    [InlineData("abc")]
    [InlineData("!!!")]
    public void ParseFlexible_WithInvalidValue_Throws(string value)
    {
        var act = () => GuidBase64Util.ParseFlexible(value);
        act.Should().Throw<Exception>();
    }

    [Fact]
    public void ToBase64_Returns22CharString()
    {
        Guid.NewGuid().ToBase64().Should().HaveLength(22);
    }

    [Fact]
    public void ToBase64_ProducesUrlSafeCharacters()
    {
        for (var i = 0; i < 100; i++)
        {
            var b64 = Guid.NewGuid().ToBase64();
            b64.Should().NotContain("+").And.NotContain("/").And.NotContain("=");
        }
    }
}