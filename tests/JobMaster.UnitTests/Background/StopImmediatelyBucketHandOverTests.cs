using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Background;

namespace JobMaster.UnitTests.Background;

/// <summary>
/// Which buckets a worker hands over (marks Lost) when it stops immediately.
/// </summary>
public class StopImmediatelyBucketHandOverTests
{
    private const string ClusterId = "test-cluster";
    private const string WorkerId = "worker-1";

    private static BucketModel Bucket(string id, BucketStatus status, string? workerId = WorkerId) => new(ClusterId)
    {
        Id = id,
        Status = status,
        AgentWorkerId = workerId,
        AgentConnectionId = new AgentConnectionId(ClusterId, "agent-1"),
    };

    [Fact]
    public void SelectBucketsToHandOver_IncludesDrainingBuckets_ExcludesLostAndReadyToDelete()
    {
        var buckets = Enum.GetValues(typeof(BucketStatus)).Cast<BucketStatus>()
            .Select(s => Bucket(s.ToString(), s))
            .ToList();

        var selected = JobMasterBackgroundAgentWorker.SelectBucketsToHandOver(buckets, WorkerId);

        selected.Select(b => b.Status).Should().BeEquivalentTo(
            Enum.GetValues(typeof(BucketStatus)).Cast<BucketStatus>()
                .Where(s => s is not (BucketStatus.Lost or BucketStatus.ReadyToDelete)));
        selected.Select(b => b.Status).Should().Contain(new[] { BucketStatus.Draining, BucketStatus.ReadyToDrain });
    }

    [Fact]
    public void SelectBucketsToHandOver_SkipsBucketsOfOtherWorkers()
    {
        var buckets = new List<BucketModel>
        {
            Bucket("mine", BucketStatus.Active),
            Bucket("other", BucketStatus.Active, workerId: "worker-2"),
            Bucket("unowned", BucketStatus.Active, workerId: null),
        };

        var selected = JobMasterBackgroundAgentWorker.SelectBucketsToHandOver(buckets, WorkerId);

        selected.Select(b => b.Id).Should().Equal("mine");
    }
}
