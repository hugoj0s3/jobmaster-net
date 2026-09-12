using FluentAssertions;
using JobMaster.Api.ApiModels;
using Microsoft.AspNetCore.Http;

namespace JobMaster.UnitTests.Api.ApiModels;

public class ApiLogItemQueryCriteriaTests
{
    [Fact]
    public void ToDomainCriteria_WithValidStandardGuid_SetsReferenceIdAsNFormat()
    {
        var guid = Guid.NewGuid();
        var criteria = new ApiLogItemQueryCriteria { ReferenceGuid = guid.ToString("D") };

        var domain = criteria.ToDomainCriteria();

        domain.ReferenceId.Should().Be(guid.ToString("N"));
    }

    [Fact]
    public void ToDomainCriteria_WithValidBase64UrlGuid_SetsReferenceIdAsNFormat()
    {
        var guid = Guid.NewGuid();
        var criteria = new ApiLogItemQueryCriteria { ReferenceGuid = guid.ToBase64() };

        var domain = criteria.ToDomainCriteria();

        domain.ReferenceId.Should().Be(guid.ToString("N"));
    }

    [Fact]
    public void ToDomainCriteria_WithInvalidReferenceGuid_ThrowsBadHttpRequestException()
    {
        var criteria = new ApiLogItemQueryCriteria { ReferenceGuid = "not-a-guid" };

        var act = () => criteria.ToDomainCriteria();

        act.Should().Throw<BadHttpRequestException>()
            .WithMessage("*not-a-guid*");
    }

    [Fact]
    public void ToDomainCriteria_WhenReferenceGuidAndReferenceIdBothSet_GuidTakesPrecedence()
    {
        var guid = Guid.NewGuid();
        var criteria = new ApiLogItemQueryCriteria
        {
            ReferenceId = "original-ref",
            ReferenceGuid = guid.ToString("D")
        };

        var domain = criteria.ToDomainCriteria();

        domain.ReferenceId.Should().Be(guid.ToString("N"));
    }

    [Fact]
    public void ToDomainCriteria_WithOnlyReferenceId_UsesItAsIs()
    {
        var criteria = new ApiLogItemQueryCriteria { ReferenceId = "my-ref-id" };

        var domain = criteria.ToDomainCriteria();

        domain.ReferenceId.Should().Be("my-ref-id");
    }

    [Fact]
    public void ToDomainCriteria_WithNullReferenceGuid_DoesNotOverwriteReferenceId()
    {
        var criteria = new ApiLogItemQueryCriteria { ReferenceId = "my-ref-id", ReferenceGuid = null };

        var domain = criteria.ToDomainCriteria();

        domain.ReferenceId.Should().Be("my-ref-id");
    }

    [Fact]
    public void ToDomainCriteria_DefaultsCountLimitTo25()
    {
        var domain = new ApiLogItemQueryCriteria().ToDomainCriteria();
        domain.CountLimit.Should().Be(25);
    }

    [Fact]
    public void ToDomainCriteria_DefaultsOffsetTo0()
    {
        var domain = new ApiLogItemQueryCriteria().ToDomainCriteria();
        domain.Offset.Should().Be(0);
    }
}