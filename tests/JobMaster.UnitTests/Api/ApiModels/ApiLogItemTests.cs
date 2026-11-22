using FluentAssertions;
using JobMaster.Api.ApiModels;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Services.Master;
using JobMaster.Sdk.Utils;

namespace JobMaster.UnitTests.Api.ApiModels;

public class ApiLogItemTests
{
    [Fact]
    public void FromDomain_WithGuidReference_ReturnsBase64()
    {
        var guid = Guid.NewGuid();

        var item = ApiLogItem.FromDomain(new LogItem { ReferenceId = guid.ToString("N") });

        item.ReferenceId.Should().Be(guid.ToBase64());
    }

    [Theory]
    [InlineData("bucket-1")]
    [InlineData("worker:host-a:1")]
    [InlineData("default")]
    public void FromDomain_WithNonGuidReference_ReturnsItUnchanged(string referenceId)
    {
        var item = ApiLogItem.FromDomain(new LogItem { ReferenceId = referenceId });

        item.ReferenceId.Should().Be(referenceId);
    }

    [Fact]
    public void FromDomain_WithNullReference_ReturnsNull()
    {
        var item = ApiLogItem.FromDomain(new LogItem { ReferenceId = null });

        item.ReferenceId.Should().BeNull();
    }

    [Fact]
    public void FromDomain_ReturnedReferenceId_AsReferenceIds_QueriesTheStoredValue()
    {
        var stored = Guid.NewGuid().ToString("N");
        var item = ApiLogItem.FromDomain(new LogItem { ReferenceId = stored });

        var domain = new ApiLogItemQueryCriteria { ReferenceIds = new[] { item.ReferenceId! } }.ToDomainCriteria();

        JobMasterLogger.ToStoredReferenceIds(domain.ReferenceIds).Should().Contain(stored);
    }
}
