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
    public void GetTimeout_WhenNoTimeoutAttributeAndNoMasterConfig_Uses5Minutes()
    {
        JobUtil.GetTimeout(typeof(MasterHandlerNoAttributes), timeout: null, masterConfig: null)
            .Should().Be(TimeSpan.FromMinutes(5));
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
    public void GetMaxNumberOfRetries_WhenNoAttributeAndNoMasterConfig_Uses3()
    {
        JobUtil.GetMaxNumberOfRetries(typeof(MasterHandlerNoAttributes), maxNumberOfRetries: null, masterConfig: null)
            .Should().Be(3);
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
}
