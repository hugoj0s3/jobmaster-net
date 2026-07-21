using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using Xunit;

namespace JobMaster.UnitTests.Sdk.Abstractions.Models.Buckets;

/// <summary>
/// Replicates JobMasterRuntime.StartAsync's exact StandaloneDrainer-synthesis condition
/// (existingBuckets.Any(x => x.IsStandaloneBucket(clusterId)) &amp;&amp; workerDefinitions.Any(), then
/// grouping by WorkerLane) against real BucketModel shapes captured from a live
/// StandaloneToDistributedTest run, to isolate whether the decision logic itself is sound
/// independent of the DB/runtime machinery around it.
/// </summary>
public class StandaloneDrainerSynthesisConditionTests
{
    private const string ClusterId = "standalone-to-dist";

    private static BucketModel StandaloneBucket(JobMasterPriority priority) => new(ClusterId)
    {
        Id = $"{ClusterId}.w1-2e9e125:{priority}:bucket-0",
        Name = $"w1-2e9e125:{priority}:bucket-0",
        AgentConnectionId = new AgentConnectionId($"{ClusterId}:{JobMasterConstants.StandaloneAgentConnName}"),
        AgentWorkerId = "w1-2e9e125",
        Priority = priority,
        Status = BucketStatus.Lost,
        WorkerLane = null,
    };

    [Fact]
    public void SynthesisCondition_EvaluatesTrue_ForFiveLostStandaloneBucketsAcrossAllPriorities()
    {
        var existingBuckets = new List<BucketModel>
        {
            StandaloneBucket(JobMasterPriority.VeryLow),
            StandaloneBucket(JobMasterPriority.Low),
            StandaloneBucket(JobMasterPriority.Medium),
            StandaloneBucket(JobMasterPriority.High),
            StandaloneBucket(JobMasterPriority.Critical),
        };
        var workerDefinitionsExist = true;

        var shouldSynthesize = existingBuckets.Any(x => x.IsStandaloneBucket(ClusterId)) && workerDefinitionsExist;
        shouldSynthesize.Should().BeTrue();

        var lanes = existingBuckets.Where(x => x.IsStandaloneBucket(ClusterId))
            .Select(x => x.WorkerLane)
            .Distinct()
            .ToList();
        lanes.Should().ContainSingle(); // all 5 buckets share the same (null) WorkerLane
    }

    [Fact]
    public void SynthesisCondition_EvaluatesFalse_WhenNoWorkerDefinitions()
    {
        var existingBuckets = new List<BucketModel> { StandaloneBucket(JobMasterPriority.Medium) };
        var workerDefinitionsExist = false;

        var shouldSynthesize = existingBuckets.Any(x => x.IsStandaloneBucket(ClusterId)) && workerDefinitionsExist;
        shouldSynthesize.Should().BeFalse();
    }

    [Fact]
    public void SynthesisCondition_EvaluatesFalse_WhenNoStandaloneBucketsExist()
    {
        var existingBuckets = new List<BucketModel>
        {
            new(ClusterId)
            {
                AgentConnectionId = new AgentConnectionId(ClusterId, "pg-agent-dist"),
                WorkerLane = null,
            },
        };
        var workerDefinitionsExist = true;

        var shouldSynthesize = existingBuckets.Any(x => x.IsStandaloneBucket(ClusterId)) && workerDefinitionsExist;
        shouldSynthesize.Should().BeFalse();
    }
}
