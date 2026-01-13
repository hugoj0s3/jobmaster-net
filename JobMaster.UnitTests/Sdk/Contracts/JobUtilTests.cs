using FluentAssertions;
using JobMaster.Contracts;
using JobMaster.Contracts.Models;
using JobMaster.Contracts.Models.Attributes;
using JobMaster.Sdk.Contracts.Jobs;
using JobMaster.Sdk.Contracts.Models;

namespace JobMaster.UnitTests.Sdk.Contracts;

public class JobUtilTests
{
    [Fact]
    public void GetJobDefinitionId_WhenAttributePresent_UsesAttribute()
    {
        JobUtil.GetJobDefinitionId(typeof(HandlerWithDefinitionId)).Should().Be("defid");
    }

    [Fact]
    public void GetJobDefinitionId_WhenAttributeMissing_UsesFullName()
    {
        JobUtil.GetJobDefinitionId(typeof(HandlerNoAttributes)).Should().Be(typeof(HandlerNoAttributes).FullName);
    }

    [Fact]
    public void GetTimeout_WhenExplicitTimeoutProvided_UsesProvidedValue()
    {
        var timeout = TimeSpan.FromSeconds(12);
        JobUtil.GetTimeout(typeof(HandlerWithTimeout), timeout, masterConfig: null).Should().Be(timeout);
    }

    [Fact]
    public void GetTimeout_WhenTimeoutAttributePresent_UsesAttribute()
    {
        JobUtil.GetTimeout(typeof(HandlerWithTimeout), timeout: null, masterConfig: null)
            .Should().Be(TimeSpan.FromSeconds(7));
    }

    [Fact]
    public void GetTimeout_WhenNoTimeoutAttribute_UsesMasterConfigDefault()
    {
        var config = new ClusterConfigurationModel("c") { DefaultJobTimeout = TimeSpan.FromSeconds(33) };
        JobUtil.GetTimeout(typeof(HandlerNoAttributes), timeout: null, masterConfig: config)
            .Should().Be(TimeSpan.FromSeconds(33));
    }

    [Fact]
    public void GetTimeout_WhenNoTimeoutAttributeAndNoMasterConfig_Uses5Minutes()
    {
        JobUtil.GetTimeout(typeof(HandlerNoAttributes), timeout: null, masterConfig: null)
            .Should().Be(TimeSpan.FromMinutes(5));
    }

    [Fact]
    public void GetWorkerLane_WhenExplicitLaneProvided_UsesProvidedLane()
    {
        JobUtil.GetWorkerLane(typeof(HandlerWithWorkerLane), workerLane: "laneX").Should().Be("laneX");
    }

    [Fact]
    public void GetWorkerLane_WhenAttributePresent_UsesAttribute()
    {
        JobUtil.GetWorkerLane(typeof(HandlerWithWorkerLane), workerLane: null).Should().Be("lane1");
    }

    [Fact]
    public void GetWorkerLane_WhenNoAttributeAndNoOverride_ReturnsNull()
    {
        JobUtil.GetWorkerLane(typeof(HandlerNoAttributes), workerLane: null).Should().BeNull();
    }

    [Fact]
    public void GetMaxNumberOfRetries_WhenExplicitProvided_UsesProvidedValue()
    {
        JobUtil.GetMaxNumberOfRetries(typeof(HandlerWithMaxRetries), maxNumberOfRetries: 2, masterConfig: null)
            .Should().Be(2);
    }

    [Fact]
    public void GetMaxNumberOfRetries_WhenAttributePresent_UsesAttribute()
    {
        JobUtil.GetMaxNumberOfRetries(typeof(HandlerWithMaxRetries), maxNumberOfRetries: null, masterConfig: null)
            .Should().Be(4);
    }

    [Fact]
    public void GetMaxNumberOfRetries_WhenNoAttribute_UsesMasterConfigDefault()
    {
        var config = new ClusterConfigurationModel("c") { DefaultMaxOfRetryCount = 5 };
        JobUtil.GetMaxNumberOfRetries(typeof(HandlerNoAttributes), maxNumberOfRetries: null, masterConfig: config)
            .Should().Be(5);
    }

    [Fact]
    public void GetMaxNumberOfRetries_WhenNoAttributeAndNoMasterConfig_Uses3()
    {
        JobUtil.GetMaxNumberOfRetries(typeof(HandlerNoAttributes), maxNumberOfRetries: null, masterConfig: null)
            .Should().Be(3);
    }

    [Fact]
    public void GetMaxNumberOfRetries_WhenResultGreaterThan10_Throws()
    {
        var act = () => JobUtil.GetMaxNumberOfRetries(typeof(HandlerWithHighRetries), maxNumberOfRetries: null, masterConfig: null);
        act.Should().Throw<ArgumentException>().WithMessage("*less than or equal to 10*");
    }

    [Fact]
    public void GetJobMasterPriority_WhenExplicitProvided_UsesProvided()
    {
        JobUtil.GetJobMasterPriority(typeof(HandlerWithPriority), JobMasterPriority.High)
            .Should().Be(JobMasterPriority.High);
    }

    [Fact]
    public void GetJobMasterPriority_WhenAttributePresent_UsesAttribute()
    {
        JobUtil.GetJobMasterPriority(typeof(HandlerWithPriority), priority: null)
            .Should().Be(JobMasterPriority.Low);
    }

    [Fact]
    public void GetJobMasterPriority_WhenNoAttributeAndNoOverride_UsesMedium()
    {
        JobUtil.GetJobMasterPriority(typeof(HandlerNoAttributes), priority: null)
            .Should().Be(JobMasterPriority.Medium);
    }

    [JobMasterDefinitionId("defid")]
    private class HandlerWithDefinitionId : IJobHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    [JobMasterTimeout(7)]
    private class HandlerWithTimeout : IJobHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    [JobMasterWorkerLane("lane1")]
    private class HandlerWithWorkerLane : IJobHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    [JobMasterMaxNumberOfRetries(4)]
    private class HandlerWithMaxRetries : IJobHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    [JobMasterMaxNumberOfRetries(11)]
    private class HandlerWithHighRetries : IJobHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    [JobMasterPriority(JobMasterPriority.Low)]
    private class HandlerWithPriority : IJobHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    private class HandlerNoAttributes : IJobHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }
}
