using FluentAssertions;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using Xunit;

namespace JobMaster.UnitTests.Sdk.Abstractions.Models.Buckets;

public class BucketModelTests
{
    private const string ClusterId = "my-cluster";

    [Fact]
    public void IsStandaloneBucket_ReturnsTrue_WhenAgentConnectionIsReservedStandaloneAndClusterMatches()
    {
        var bucket = new BucketModel(ClusterId)
        {
            AgentConnectionId = new AgentConnectionId(ClusterId, JobMasterConstants.StandaloneAgentConnName),
        };

        bucket.IsStandaloneBucket(ClusterId).Should().BeTrue();
    }

    [Fact]
    public void IsStandaloneBucket_ReturnsTrue_WhenConstructedFromCompositeIdString()
    {
        var bucket = new BucketModel(ClusterId)
        {
            AgentConnectionId = new AgentConnectionId($"{ClusterId}:{JobMasterConstants.StandaloneAgentConnName}"),
        };

        bucket.IsStandaloneBucket(ClusterId).Should().BeTrue();
    }

    [Fact]
    public void IsStandaloneBucket_ReturnsFalse_WhenAgentConnectionIsARealNamedConnection()
    {
        var bucket = new BucketModel(ClusterId)
        {
            AgentConnectionId = new AgentConnectionId(ClusterId, "pg-agent-dist"),
        };

        bucket.IsStandaloneBucket(ClusterId).Should().BeFalse();
    }

    [Fact]
    public void IsStandaloneBucket_ReturnsFalse_WhenClusterIdDoesNotMatch()
    {
        var bucket = new BucketModel(ClusterId)
        {
            AgentConnectionId = new AgentConnectionId(ClusterId, JobMasterConstants.StandaloneAgentConnName),
        };

        bucket.IsStandaloneBucket("a-different-cluster").Should().BeFalse();
    }
}
