using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Utils;
using Xunit;

namespace JobMaster.UnitTests.Sdk.Abstractions.Models.Jobs;

public class JobRawModelTests
{
    // A fresh random ClusterId per bucket avoids collisions in JobMasterClusterConnectionConfig's
    // static registry across tests -- same pattern JobsExecutionEngineFixture uses -- since
    // BucketModel.CanAssign() requires AgentConnectionId.IsValid(), which looks up a real
    // registered, ready cluster connection config for the bucket's ClusterId.
    private static BucketModel NewAssignableBucket()
    {
        var clusterId = $"c{JobMasterRandomUtil.NewGuid4():N}";
        var clusterConfig = JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: false);
        clusterConfig.MarkAsReady();

        return new BucketModel(clusterId)
        {
            Id = "bucket-1",
            AgentConnectionId = new AgentConnectionId(clusterId, "agent-1"),
            AgentWorkerId = "worker-1",
            HostId = new HostId(clusterId, "host-1")
        };
    }

    [Fact]
    public void AssignSavePendingJobToBucket_DoesNotChangeStatus()
    {
        var bucket = NewAssignableBucket();
        var job = new JobRawModel(bucket.ClusterId) { Status = JobMasterJobStatus.PendingSave };

        job.AssignSavePendingJobToBucket(bucket);

        job.Status.Should().Be(JobMasterJobStatus.PendingSave,
            "JobSavePendingOperation.AddSavePendingJobAsync's short-circuit fast path checks " +
            "Status == PendingSave to decide whether it can inject the job directly into the " +
            "execution engine on the same worker; flipping Status here (as it previously did, to " +
            "InBucket) makes that check permanently false for every scheduled job, silently " +
            "forcing all of them onto the slower publish-then-pull queue path instead.");
        job.BucketId.Should().Be(bucket.Id);
        job.AgentConnectionId.Should().Be(bucket.AgentConnectionId);
        job.AgentWorkerId.Should().Be(bucket.AgentWorkerId);
        job.HostId.Should().Be(bucket.HostId);
    }

    [Fact]
    public void AssignToBucket_SetsStatusToInBucket()
    {
        var bucket = NewAssignableBucket();
        var job = new JobRawModel(bucket.ClusterId) { Status = JobMasterJobStatus.OnMaster };

        job.AssignToBucket(bucket);

        job.Status.Should().Be(JobMasterJobStatus.InBucket,
            "unlike AssignSavePendingJobToBucket, this is the real/final bucket assignment " +
            "(also refreshes the process deadline) and should transition the job into InBucket.");
        job.BucketId.Should().Be(bucket.Id);
    }
}
