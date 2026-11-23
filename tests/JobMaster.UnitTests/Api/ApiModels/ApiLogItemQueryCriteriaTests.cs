using FluentAssertions;
using JobMaster.Api.ApiModels;

namespace JobMaster.UnitTests.Api.ApiModels;

public class ApiLogItemQueryCriteriaTests
{
    [Fact]
    public void ToDomainCriteria_PassesReferenceIdsAsGiven_SkippingEmptyOnes()
    {
        var criteria = new ApiLogItemQueryCriteria { ReferenceIds = new[] { "bucket-1:abc", "", "zWm0Uu1oV0K7c3dq8aVJpA" } };

        var domain = criteria.ToDomainCriteria();

        // Matching every stored form is the logger's job (JobMasterLogger.ToStoredReferenceIds).
        domain.ReferenceIds.Should().Equal("bucket-1:abc", "zWm0Uu1oV0K7c3dq8aVJpA");
    }

    [Fact]
    public void ToDomainCriteria_WithoutReferenceIds_HasNone()
    {
        new ApiLogItemQueryCriteria().ToDomainCriteria().ReferenceIds.Should().BeEmpty();
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