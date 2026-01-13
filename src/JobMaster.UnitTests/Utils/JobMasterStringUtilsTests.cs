using FluentAssertions;
using JobMaster.Contracts.Utils;

namespace JobMaster.UnitTests.Utils;

public class JobMasterStringUtilsTests
{
    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("abc def")]
    [InlineData("abc@def")]
    [InlineData("abc..def")]
    [InlineData("abc::def")]
    [InlineData("abc.:def")]
    [InlineData("abc:.")]
    [InlineData("ab__cd")]
    [InlineData("ab--cd")]
    public void IsValidForId_WhenValueIsInvalid_ShouldReturnFalse(string value)
    {
        JobMasterStringUtils.IsValidForId(value).Should().BeFalse();
    }

    [Fact]
    public void IsValidForId_WhenSegmentIsTooLong_ShouldReturnFalse()
    {
        var segment76 = new string('a', 76);
        JobMasterStringUtils.IsValidForId(segment76).Should().BeFalse();
    }

    [Fact]
    public void IsValidForId_WhenValueIsTooLong_ShouldReturnFalse()
    {
        var id501 = new string('a', 501);
        JobMasterStringUtils.IsValidForId(id501).Should().BeFalse();
    }

    [Fact]
    public void IsValidForId_WhenValueIsValid_ShouldReturnTrue()
    {
        JobMasterStringUtils.IsValidForId("cluster_1.repo-2:worker_3").Should().BeTrue();
    }
}
