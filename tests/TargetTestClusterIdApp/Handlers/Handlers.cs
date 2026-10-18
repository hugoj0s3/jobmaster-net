using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Abstractions.StaticRecurringSchedules;
using TargetTestClusterIdApp.Redis;

namespace TargetTestClusterIdApp.Handlers;

// One-off via [JobMasterClusterId] -- scheduled without a clusterId, must land on the target cluster.
[JobMasterDefinitionId("ClusterIdApp.AttributeOnce")]
[JobMasterClusterId(ClusterIds.Target)]
public sealed class AttributeOnceHandler(ExecutionRecorder recorder) : IJobMasterHandler
{
    public Task HandleAsync(JobContext job) =>
        recorder.RecordAsync(job.Metadata.GetStringValue("TestIdentifier"), job.Id, "ClusterIdApp.AttributeOnce", job.ClusterId);
}

// One-off via Advanced.OnceNowAsync<TDefinition>() -- the cluster comes from the definition's Config.ClusterId.
public sealed class AdvancedOnceDefinitionAttribute : JobDefinitionConfigAttribute, IStaticJobDefinitionConfig
{
    public static JobDefinitionConfig Config { get; } = new JobDefinitionConfig("ClusterIdApp.AdvancedOnce", clusterId: ClusterIds.Target);
}

[AdvancedOnceDefinitionAttribute]
public sealed class AdvancedOnceHandler(ExecutionRecorder recorder) : IJobMasterHandler
{
    public Task HandleAsync(JobContext job) =>
        recorder.RecordAsync(job.Metadata.GetStringValue("TestIdentifier"), job.Id, "ClusterIdApp.AdvancedOnce", job.ClusterId);
}

// Control: no cluster id anywhere -- must land on the default cluster, proving the assertions can tell the
// two clusters apart rather than passing because everything ended up in one place.
[JobMasterDefinitionId("ClusterIdApp.DefaultOnce")]
public sealed class DefaultOnceHandler(ExecutionRecorder recorder) : IJobMasterHandler
{
    public Task HandleAsync(JobContext job) =>
        recorder.RecordAsync(job.Metadata.GetStringValue("TestIdentifier"), job.Id, "ClusterIdApp.DefaultOnce", job.ClusterId);
}

// Static recurring via [JobMasterClusterId] + schedule attribute, no profile -- the synthesized attribute
// profile has no ClusterId of its own, so the handler's cluster id must win over the default.
[JobMasterDefinitionId("ClusterIdApp.AttributeRecurring")]
[JobMasterClusterId(ClusterIds.Target)]
[TimeSpanIntervalSchedule("00:00:15")]
public sealed class AttributeRecurringHandler(ExecutionRecorder recorder) : IJobMasterHandler
{
    public const string TestIdentifier = "cluster-id-attribute-recurring";

    public Task HandleAsync(JobContext job) =>
        recorder.RecordAsync(TestIdentifier, job.Id, "ClusterIdApp.AttributeRecurring", job.ClusterId);
}

// Plain handler, registered by TargetClusterProfile -- the cluster comes only from the profile's ClusterId.
[JobMasterDefinitionId("ClusterIdApp.ProfileTick")]
public sealed class ProfileTickHandler(ExecutionRecorder recorder) : IJobMasterHandler
{
    public Task HandleAsync(JobContext job) =>
        recorder.RecordAsync(job.Metadata.GetStringValue("TestIdentifier"), job.Id, "ClusterIdApp.ProfileTick", job.ClusterId);
}
