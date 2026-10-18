using FluentAssertions;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models;

namespace JobMaster.UnitTests.Sdk.Contracts;

public class JobUtilTests
{
    [Fact]
    public void GetJobDefinitionId_WhenAttributePresent_UsesAttribute()
    {
        JobUtil.GetJobDefinitionId(typeof(MasterHandlerWithDefinitionId)).Should().Be("defid");
    }

    [Fact]
    public void GetJobDefinitionId_WhenAttributeMissing_UsesFullName()
    {
        JobUtil.GetJobDefinitionId(typeof(MasterHandlerNoAttributes)).Should().Be(typeof(MasterHandlerNoAttributes).FullName);
    }

    [Fact]
    public void GetTimeout_WhenJobDefinitionConfigAttributePresent_UsesConfigOverIndividualAttribute()
    {
        JobUtil.GetTimeout(typeof(MasterHandlerWithConfigAndIndividualAttributes), timeout: null, masterConfig: null)
            .Should().Be(TimeSpan.FromSeconds(21));
    }

    [Fact]
    public void GetWorkerLane_WhenJobDefinitionConfigAttributePresent_UsesConfigOverIndividualAttribute()
    {
        JobUtil.GetWorkerLane(typeof(MasterHandlerWithConfigAndIndividualAttributes), workerLane: null)
            .Should().Be("config-lane");
    }

    [Fact]
    public void GetMaxNumberOfRetries_WhenJobDefinitionConfigAttributePresent_UsesConfigOverIndividualAttribute()
    {
        JobUtil.GetMaxNumberOfRetries(typeof(MasterHandlerWithConfigAndIndividualAttributes), maxNumberOfRetries: null, masterConfig: null)
            .Should().Be(6);
    }

    [Fact]
    public void GetJobMasterPriority_WhenJobDefinitionConfigAttributePresent_UsesConfigOverIndividualAttribute()
    {
        JobUtil.GetJobMasterPriority(typeof(MasterHandlerWithConfigAndIndividualAttributes), priority: null)
            .Should().Be(JobMasterPriority.Critical);
    }

    [Fact]
    public void GetTimeout_WhenExplicitTimeoutProvided_UsesProvidedValue()
    {
        var timeout = TimeSpan.FromSeconds(12);
        JobUtil.GetTimeout(typeof(MasterHandlerWithTimeout), timeout, masterConfig: null).Should().Be(timeout);
    }

    [Fact]
    public void GetTimeout_WhenTimeoutAttributePresent_UsesAttribute()
    {
        JobUtil.GetTimeout(typeof(MasterHandlerWithTimeout), timeout: null, masterConfig: null)
            .Should().Be(TimeSpan.FromSeconds(7));
    }

    [Fact]
    public void GetTimeout_WhenNoTimeoutAttribute_UsesMasterConfigDefault()
    {
        var config = new ClusterConfigurationModel("c") { DefaultJobTimeout = TimeSpan.FromSeconds(33) };
        JobUtil.GetTimeout(typeof(MasterHandlerNoAttributes), timeout: null, masterConfig: config)
            .Should().Be(TimeSpan.FromSeconds(33));
    }

    [Fact]
    public void GetTimeout_WhenNoTimeoutAttributeAndNoMasterConfig_UsesJobMasterDefault()
    {
        JobUtil.GetTimeout(typeof(MasterHandlerNoAttributes), timeout: null, masterConfig: null)
            .Should().Be(JobMasterDefaults.DefaultJobTimeout);
    }

    [Fact]
    public void GetWorkerLane_WhenExplicitLaneProvided_UsesProvidedLane()
    {
        JobUtil.GetWorkerLane(typeof(MasterHandlerWithWorkerLane), workerLane: "laneX").Should().Be("laneX");
    }

    [Fact]
    public void GetWorkerLane_WhenAttributePresent_UsesAttribute()
    {
        JobUtil.GetWorkerLane(typeof(MasterHandlerWithWorkerLane), workerLane: null).Should().Be("lane1");
    }

    [Fact]
    public void GetWorkerLane_WhenNoAttributeAndNoOverride_ReturnsNull()
    {
        JobUtil.GetWorkerLane(typeof(MasterHandlerNoAttributes), workerLane: null).Should().BeNull();
    }

    [Fact]
    public void GetClusterId_WhenExplicitProvided_UsesProvidedValue()
    {
        JobUtil.GetClusterId(typeof(MasterHandlerWithClusterId), clusterId: "explicit").Should().Be("explicit");
    }

    [Fact]
    public void GetClusterId_WhenAttributePresent_UsesAttribute()
    {
        JobUtil.GetClusterId(typeof(MasterHandlerWithClusterId), clusterId: null).Should().Be("attr-cluster");
    }

    [Fact]
    public void GetClusterId_WhenJobDefinitionConfigAttributePresent_UsesConfigOverIndividualAttribute()
    {
        JobUtil.GetClusterId(typeof(MasterHandlerWithConfigAndIndividualAttributes), clusterId: null)
            .Should().Be("config-cluster");
    }

    [Fact]
    public void GetClusterId_WhenNoAttributeAndNoOverride_ReturnsNull()
    {
        JobUtil.GetClusterId(typeof(MasterHandlerNoAttributes), clusterId: null).Should().BeNull();
    }

    [Fact]
    public void GetMaxNumberOfRetries_WhenExplicitProvided_UsesProvidedValue()
    {
        JobUtil.GetMaxNumberOfRetries(typeof(MasterHandlerWithMaxRetries), maxNumberOfRetries: 2, masterConfig: null)
            .Should().Be(2);
    }

    [Fact]
    public void GetMaxNumberOfRetries_WhenAttributePresent_UsesAttribute()
    {
        JobUtil.GetMaxNumberOfRetries(typeof(MasterHandlerWithMaxRetries), maxNumberOfRetries: null, masterConfig: null)
            .Should().Be(4);
    }

    [Fact]
    public void GetMaxNumberOfRetries_WhenNoAttribute_UsesMasterConfigDefault()
    {
        var config = new ClusterConfigurationModel("c") { DefaultMaxOfRetryCount = 5 };
        JobUtil.GetMaxNumberOfRetries(typeof(MasterHandlerNoAttributes), maxNumberOfRetries: null, masterConfig: config)
            .Should().Be(5);
    }

    [Fact]
    public void GetMaxNumberOfRetries_WhenNoAttributeAndNoMasterConfig_UsesJobMasterDefault()
    {
        JobUtil.GetMaxNumberOfRetries(typeof(MasterHandlerNoAttributes), maxNumberOfRetries: null, masterConfig: null)
            .Should().Be(JobMasterDefaults.MaxRetryCount);
    }

    [Fact]
    public void GetMaxNumberOfRetries_WhenResultGreaterThan10_Throws()
    {
        var act = () => JobUtil.GetMaxNumberOfRetries(typeof(MasterHandlerWithHighRetries), maxNumberOfRetries: null, masterConfig: null);
        act.Should().Throw<ArgumentException>().WithMessage("*less than or equal to 10*");
    }

    [Fact]
    public void GetJobMasterPriority_WhenExplicitProvided_UsesProvided()
    {
        JobUtil.GetJobMasterPriority(typeof(MasterHandlerWithPriority), JobMasterPriority.High)
            .Should().Be(JobMasterPriority.High);
    }

    [Fact]
    public void GetJobMasterPriority_WhenAttributePresent_UsesAttribute()
    {
        JobUtil.GetJobMasterPriority(typeof(MasterHandlerWithPriority), priority: null)
            .Should().Be(JobMasterPriority.Low);
    }

    [Fact]
    public void GetJobMasterPriority_WhenNoAttributeAndNoOverride_UsesMedium()
    {
        JobUtil.GetJobMasterPriority(typeof(MasterHandlerNoAttributes), priority: null)
            .Should().Be(JobMasterPriority.Medium);
    }

    // Classic attributes are inherited from a base class (like JobDefinitionConfigAttribute); a derived class's own
    // attribute wins. [JobMasterMetadata] (AllowMultiple) is merged across the hierarchy, derived key wins.

    [Fact]
    public void ClassicAttributes_AreInheritedFromBaseClass()
    {
        var t = typeof(DerivedHandlerInheritingAll);

        JobUtil.GetClusterId(t, clusterId: null).Should().Be("base-cluster");
        JobUtil.GetJobMasterPriority(t, priority: null).Should().Be(JobMasterPriority.High);
        JobUtil.GetWorkerLane(t, workerLane: null).Should().Be("base-lane");
        JobUtil.GetTimeout(t, timeout: null, masterConfig: null).Should().Be(TimeSpan.FromSeconds(17));
        JobUtil.GetMaxNumberOfRetries(t, maxNumberOfRetries: null, masterConfig: null).Should().Be(4);
    }

    [Fact]
    public void ClassicAttributes_DerivedClassAttributeOverridesBase()
    {
        var t = typeof(DerivedHandlerOverriding);

        JobUtil.GetClusterId(t, clusterId: null).Should().Be("derived-cluster");
        JobUtil.GetJobMasterPriority(t, priority: null).Should().Be(JobMasterPriority.Low);
        JobUtil.GetWorkerLane(t, workerLane: null).Should().Be("base-lane");
    }

    [Fact]
    public void MetadataAttribute_IsInheritedFromBaseClass()
    {
        var job = Job.New("cluster", typeof(DerivedHandlerInheritingAll));

        job.Metadata!.ToReadable().GetStringValue("base-key").Should().Be("base-value");
    }

    [Fact]
    public void MetadataAttribute_DerivedKeyOverridesBaseAndOtherKeysMerge()
    {
        var metadata = JobUtil.GetHandlerMetadata(typeof(DerivedHandlerOverriding));

        metadata["base-key"].Should().Be("derived-value");
        metadata["derived-only"].Should().Be("x");
        metadata["base-only"].Should().Be("y");
    }

    [Fact]
    public void MetadataAttribute_CallMetadataOverridesInheritedAttribute()
    {
        var job = Job.New("cluster", typeof(DerivedHandlerOverriding),
            writableMetadata: WritableMetadata.New().SetStringValue("base-key", "from-call"));

        job.Metadata!.ToReadable().GetStringValue("base-key").Should().Be("from-call");
    }

    [JobMasterClusterId("base-cluster")]
    [JobMasterPriority(JobMasterPriority.High)]
    [JobMasterWorkerLane("base-lane")]
    [JobMasterTimeout(17)]
    [JobMasterMaxNumberOfRetries(4)]
    [JobMasterMetadata("base-key", "base-value")]
    [JobMasterMetadata("base-only", "y")]
    private abstract class BaseHandlerWithAttributes : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    private sealed class DerivedHandlerInheritingAll : BaseHandlerWithAttributes;

    [JobMasterClusterId("derived-cluster")]
    [JobMasterPriority(JobMasterPriority.Low)]
    [JobMasterMetadata("base-key", "derived-value")]
    [JobMasterMetadata("derived-only", "x")]
    private sealed class DerivedHandlerOverriding : BaseHandlerWithAttributes;

    [JobMasterDefinitionId("defid")]
    private class MasterHandlerWithDefinitionId : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    [JobMasterTimeout(7)]
    private class MasterHandlerWithTimeout : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    [JobMasterWorkerLane("lane1")]
    private class MasterHandlerWithWorkerLane : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    [JobMasterClusterId("attr-cluster")]
    private class MasterHandlerWithClusterId : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    [JobMasterMaxNumberOfRetries(4)]
    private class MasterHandlerWithMaxRetries : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    [JobMasterMaxNumberOfRetries(11)]
    private class MasterHandlerWithHighRetries : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    [JobMasterPriority(JobMasterPriority.Low)]
    private class MasterHandlerWithPriority : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    private class MasterHandlerNoAttributes : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    private class FakeDefinitionAttribute : JobDefinitionConfigAttribute, IStaticJobDefinitionConfig
    {
        public static JobDefinitionConfig Config { get; } = new JobDefinitionConfig(
            "config-defid",
            priority: JobMasterPriority.Critical,
            timeout: TimeSpan.FromSeconds(21),
            maxNumberOfRetries: 6,
            workerLane: "config-lane",
            clusterId: "config-cluster");
    }

    [FakeDefinitionAttribute]
    [JobMasterTimeout(99)]
    [JobMasterWorkerLane("individual-lane")]
    [JobMasterClusterId("individual-cluster")]
    [JobMasterMaxNumberOfRetries(9)]
    [JobMasterPriority(JobMasterPriority.Low)]
    private class MasterHandlerWithConfigAndIndividualAttributes : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }
}
