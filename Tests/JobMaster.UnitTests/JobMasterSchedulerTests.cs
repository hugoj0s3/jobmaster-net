using System.Reflection;
using FluentAssertions;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Services;
using JobMaster.Sdk.Abstractions.Services.Master;
using Moq;

namespace JobMaster.UnitTests;

public class JobMasterSchedulerTests
{
    
    [Fact]
    public void OnceNow_WhenClusterIdNull_ShouldUseDefaultClusterId_AndSchedule()
    {
        var clusterId = "c";

        using var _ = new StaticStateScope(new FakeRuntime(started: true));

        JobMasterClusterConnectionConfig.Create(
            clusterId: clusterId,
            repositoryTypeId: "repo",
            connectionString: "cnn",
            isDefault: true);

        var schedulerMock = new Mock<IJobMasterSchedulerClusterAware>(MockBehavior.Strict);
        var configServiceMock = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);

        configServiceMock
            .Setup(x => x.Get())
            .Returns(new ClusterConfigurationModel(clusterId));

        schedulerMock
            .Setup(x => x.Schedule(It.Is<JobRawModel>(m => m.ClusterId == clusterId)))
            .Verifiable();

        var factoryMock = new Mock<IJobMasterClusterAwareComponentFactory>(MockBehavior.Strict);
        factoryMock.SetupGet(x => x.ClusterId).Returns(clusterId);
        factoryMock
            .Setup(x => x.GetComponent<IJobMasterSchedulerClusterAware>())
            .Returns(schedulerMock.Object);
        factoryMock
            .Setup(x => x.GetComponent<IMasterClusterConfigurationService>())
            .Returns(configServiceMock.Object);

        JobMasterClusterAwareComponentFactories.AddFactory(clusterId, factoryMock.Object);

        var ctx = JobMasterScheduler.Instance.OnceNow<TestJobMasterHandler>();

        ctx.ClusterId.Should().Be(clusterId);
        schedulerMock.Verify();
        configServiceMock.Verify();
    }

    [Fact]
    public async Task OnceNowAsync_WhenClusterIdNull_ShouldUseDefaultClusterId_AndScheduleAsync()
    {
        var clusterId = "c";

        using var _ = new StaticStateScope(new FakeRuntime(started: true));

        JobMasterClusterConnectionConfig.Create(
            clusterId: clusterId,
            repositoryTypeId: "repo",
            connectionString: "cnn",
            isDefault: true);

        var schedulerMock = new Mock<IJobMasterSchedulerClusterAware>(MockBehavior.Strict);
        var configServiceMock = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);

        configServiceMock
            .Setup(x => x.Get())
            .Returns(new ClusterConfigurationModel(clusterId));

        schedulerMock
            .Setup(x => x.ScheduleAsync(It.Is<JobRawModel>(m => m.ClusterId == clusterId)))
            .Returns(Task.CompletedTask)
            .Verifiable();

        var factoryMock = new Mock<IJobMasterClusterAwareComponentFactory>(MockBehavior.Strict);
        factoryMock.SetupGet(x => x.ClusterId).Returns(clusterId);
        factoryMock
            .Setup(x => x.GetComponent<IJobMasterSchedulerClusterAware>())
            .Returns(schedulerMock.Object);
        factoryMock
            .Setup(x => x.GetComponent<IMasterClusterConfigurationService>())
            .Returns(configServiceMock.Object);

        JobMasterClusterAwareComponentFactories.AddFactory(clusterId, factoryMock.Object);

        var ctx = await JobMasterScheduler.Instance.OnceNowAsync<TestJobMasterHandler>();

        ctx.ClusterId.Should().Be(clusterId);
        schedulerMock.Verify();
        configServiceMock.Verify();
    }

    [Fact]
    public void OnceNow_WhenExplicitPriorityIsDisabled_Throws()
    {
        var clusterId = "c-disabled-explicit";

        using var _ = new StaticStateScope(new FakeRuntime(started: true));

        var clusterCfg = JobMasterClusterConnectionConfig.Create(
            clusterId: clusterId,
            repositoryTypeId: "repo",
            connectionString: "cnn",
            isDefault: true);
        clusterCfg.SetDisabledPriorities(new HashSet<JobMasterPriority> { JobMasterPriority.VeryLow });

        var configServiceMock = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);
        configServiceMock
            .Setup(x => x.Get())
            .Returns(new ClusterConfigurationModel(clusterId));

        var schedulerMock = new Mock<IJobMasterSchedulerClusterAware>(MockBehavior.Strict);

        var factoryMock = new Mock<IJobMasterClusterAwareComponentFactory>(MockBehavior.Strict);
        factoryMock.SetupGet(x => x.ClusterId).Returns(clusterId);
        factoryMock
            .Setup(x => x.GetComponent<IJobMasterSchedulerClusterAware>())
            .Returns(schedulerMock.Object);
        factoryMock
            .Setup(x => x.GetComponent<IMasterClusterConfigurationService>())
            .Returns(configServiceMock.Object);

        JobMasterClusterAwareComponentFactories.AddFactory(clusterId, factoryMock.Object);

        JobMasterScheduler.Instance
            .Invoking(s => s.OnceNow<TestJobMasterHandler>(priority: JobMasterPriority.VeryLow))
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*VeryLow*disabled*");
    }

    [Fact]
    public void OnceNow_WhenHandlerAttributePriorityIsDisabled_Throws()
    {
        var clusterId = "c-disabled-attr";

        using var _ = new StaticStateScope(new FakeRuntime(started: true));

        var clusterCfg = JobMasterClusterConnectionConfig.Create(
            clusterId: clusterId,
            repositoryTypeId: "repo",
            connectionString: "cnn",
            isDefault: true);
        clusterCfg.SetDisabledPriorities(new HashSet<JobMasterPriority> { JobMasterPriority.Critical });

        var configServiceMock = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);
        configServiceMock
            .Setup(x => x.Get())
            .Returns(new ClusterConfigurationModel(clusterId));

        var schedulerMock = new Mock<IJobMasterSchedulerClusterAware>(MockBehavior.Strict);

        var factoryMock = new Mock<IJobMasterClusterAwareComponentFactory>(MockBehavior.Strict);
        factoryMock.SetupGet(x => x.ClusterId).Returns(clusterId);
        factoryMock
            .Setup(x => x.GetComponent<IJobMasterSchedulerClusterAware>())
            .Returns(schedulerMock.Object);
        factoryMock
            .Setup(x => x.GetComponent<IMasterClusterConfigurationService>())
            .Returns(configServiceMock.Object);

        JobMasterClusterAwareComponentFactories.AddFactory(clusterId, factoryMock.Object);

        // CriticalPriorityHandler has [JobMasterPriority(Critical)], no explicit priority arg
        JobMasterScheduler.Instance
            .Invoking(s => s.OnceNow<CriticalPriorityHandler>())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*Critical*disabled*");
    }

    [Fact]
    public void OnceNow_WhenPriorityIsNotDisabled_Schedules()
    {
        var clusterId = "c-enabled-priority";

        using var _ = new StaticStateScope(new FakeRuntime(started: true));

        var clusterCfg = JobMasterClusterConnectionConfig.Create(
            clusterId: clusterId,
            repositoryTypeId: "repo",
            connectionString: "cnn",
            isDefault: true);
        clusterCfg.SetDisabledPriorities(new HashSet<JobMasterPriority> { JobMasterPriority.VeryLow });

        var configServiceMock = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);
        configServiceMock
            .Setup(x => x.Get())
            .Returns(new ClusterConfigurationModel(clusterId));

        var schedulerMock = new Mock<IJobMasterSchedulerClusterAware>(MockBehavior.Strict);
        schedulerMock
            .Setup(x => x.Schedule(It.IsAny<JobRawModel>()))
            .Verifiable();

        var factoryMock = new Mock<IJobMasterClusterAwareComponentFactory>(MockBehavior.Strict);
        factoryMock.SetupGet(x => x.ClusterId).Returns(clusterId);
        factoryMock
            .Setup(x => x.GetComponent<IJobMasterSchedulerClusterAware>())
            .Returns(schedulerMock.Object);
        factoryMock
            .Setup(x => x.GetComponent<IMasterClusterConfigurationService>())
            .Returns(configServiceMock.Object);

        JobMasterClusterAwareComponentFactories.AddFactory(clusterId, factoryMock.Object);

        // Medium (default) is not disabled — should succeed
        JobMasterScheduler.Instance
            .Invoking(s => s.OnceNow<TestJobMasterHandler>())
            .Should().NotThrow();

        schedulerMock.Verify();
    }

    [Fact]
    public void AdvancedOnceNow_WithDefinitionAttribute_SchedulesUsingConfigValues()
    {
        var clusterId = "c-advanced-attr";

        using var _ = new StaticStateScope(new FakeRuntime(started: true));

        JobMasterClusterConnectionConfig.Create(
            clusterId: clusterId,
            repositoryTypeId: "repo",
            connectionString: "cnn",
            isDefault: true);

        var configServiceMock = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);
        configServiceMock.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId));

        var schedulerMock = new Mock<IJobMasterSchedulerClusterAware>(MockBehavior.Strict);
        JobRawModel? scheduled = null;
        schedulerMock
            .Setup(x => x.Schedule(It.IsAny<JobRawModel>()))
            .Callback<JobRawModel>(m => scheduled = m)
            .Verifiable();

        var factoryMock = new Mock<IJobMasterClusterAwareComponentFactory>(MockBehavior.Strict);
        factoryMock.SetupGet(x => x.ClusterId).Returns(clusterId);
        factoryMock.Setup(x => x.GetComponent<IJobMasterSchedulerClusterAware>()).Returns(schedulerMock.Object);
        factoryMock.Setup(x => x.GetComponent<IMasterClusterConfigurationService>()).Returns(configServiceMock.Object);

        JobMasterClusterAwareComponentFactories.AddFactory(clusterId, factoryMock.Object);

        var ctx = JobMasterScheduler.Instance.Advanced.OnceNow<TestDefinitionAttribute>(clusterId: clusterId);

        ctx.ClusterId.Should().Be(clusterId);
        scheduled.Should().NotBeNull();
        scheduled!.JobDefinitionId.Should().Be("advanced-defid");
        scheduled.Priority.Should().Be(JobMasterPriority.High);
        scheduled.Timeout.Should().Be(TimeSpan.FromSeconds(42));
        scheduled.WorkerLane.Should().Be("advanced-lane");
        schedulerMock.Verify();
    }

    [Fact]
    public void AdvancedOnceNow_WithConfigObject_SchedulesUsingConfigValues()
    {
        var clusterId = "c-advanced-config";

        using var _ = new StaticStateScope(new FakeRuntime(started: true));

        JobMasterClusterConnectionConfig.Create(
            clusterId: clusterId,
            repositoryTypeId: "repo",
            connectionString: "cnn",
            isDefault: true);

        var configServiceMock = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);
        configServiceMock.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId));

        var schedulerMock = new Mock<IJobMasterSchedulerClusterAware>(MockBehavior.Strict);
        JobRawModel? scheduled = null;
        schedulerMock
            .Setup(x => x.Schedule(It.IsAny<JobRawModel>()))
            .Callback<JobRawModel>(m => scheduled = m)
            .Verifiable();

        var factoryMock = new Mock<IJobMasterClusterAwareComponentFactory>(MockBehavior.Strict);
        factoryMock.SetupGet(x => x.ClusterId).Returns(clusterId);
        factoryMock.Setup(x => x.GetComponent<IJobMasterSchedulerClusterAware>()).Returns(schedulerMock.Object);
        factoryMock.Setup(x => x.GetComponent<IMasterClusterConfigurationService>()).Returns(configServiceMock.Object);

        JobMasterClusterAwareComponentFactories.AddFactory(clusterId, factoryMock.Object);

        var config = new JobDefinitionConfig(
            "orders.process",
            priority: JobMasterPriority.Low,
            timeout: TimeSpan.FromSeconds(10),
            workerLane: "orders");

        var ctx = JobMasterScheduler.Instance.Advanced.OnceNow(config, clusterId: clusterId);

        ctx.ClusterId.Should().Be(clusterId);
        scheduled.Should().NotBeNull();
        scheduled!.JobDefinitionId.Should().Be("orders.process");
        scheduled.Priority.Should().Be(JobMasterPriority.Low);
        scheduled.Timeout.Should().Be(TimeSpan.FromSeconds(10));
        scheduled.WorkerLane.Should().Be("orders");
        schedulerMock.Verify();
    }

    [Fact]
    public void AdvancedOnceNow_WithDefinitionAttribute_PerCallOverrideTakesPrecedenceOverAttributeConfig()
    {
        // Only the TDefinition-generic overload accepts per-call overrides — the JobDefinitionConfig
        // overload does not, since the caller already builds that object with the values they want.
        var clusterId = "c-advanced-override";

        using var _ = new StaticStateScope(new FakeRuntime(started: true));

        JobMasterClusterConnectionConfig.Create(
            clusterId: clusterId,
            repositoryTypeId: "repo",
            connectionString: "cnn",
            isDefault: true);

        var configServiceMock = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);
        configServiceMock.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId));

        var schedulerMock = new Mock<IJobMasterSchedulerClusterAware>(MockBehavior.Strict);
        JobRawModel? scheduled = null;
        schedulerMock
            .Setup(x => x.Schedule(It.IsAny<JobRawModel>()))
            .Callback<JobRawModel>(m => scheduled = m)
            .Verifiable();

        var factoryMock = new Mock<IJobMasterClusterAwareComponentFactory>(MockBehavior.Strict);
        factoryMock.SetupGet(x => x.ClusterId).Returns(clusterId);
        factoryMock.Setup(x => x.GetComponent<IJobMasterSchedulerClusterAware>()).Returns(schedulerMock.Object);
        factoryMock.Setup(x => x.GetComponent<IMasterClusterConfigurationService>()).Returns(configServiceMock.Object);

        JobMasterClusterAwareComponentFactories.AddFactory(clusterId, factoryMock.Object);

        // TestDefinitionAttribute.Config declares Priority = High, Timeout = 42s.
        JobMasterScheduler.Instance.Advanced.OnceNow<TestDefinitionAttribute>(
            priority: JobMasterPriority.Critical,
            timeout: TimeSpan.FromSeconds(99),
            clusterId: clusterId);

        scheduled.Should().NotBeNull();
        scheduled!.JobDefinitionId.Should().Be("advanced-defid");
        scheduled.Priority.Should().Be(JobMasterPriority.Critical);
        scheduled.Timeout.Should().Be(TimeSpan.FromSeconds(99));
    }

    [Fact]
    public void AdvancedRecurring_WithDefinitionAttribute_SchedulesUsingConfigValues()
    {
        var clusterId = "c-advanced-recurring-attr";

        using var _ = new StaticStateScope(new FakeRuntime(started: true));

        JobMasterClusterConnectionConfig.Create(
            clusterId: clusterId,
            repositoryTypeId: "repo",
            connectionString: "cnn",
            isDefault: true);

        var configServiceMock = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);
        configServiceMock.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId));

        var schedulerMock = new Mock<IJobMasterSchedulerClusterAware>(MockBehavior.Strict);
        RecurringScheduleRawModel? scheduled = null;
        schedulerMock
            .Setup(x => x.Schedule(It.IsAny<RecurringScheduleRawModel>()))
            .Callback<RecurringScheduleRawModel>(m => scheduled = m)
            .Verifiable();

        var factoryMock = new Mock<IJobMasterClusterAwareComponentFactory>(MockBehavior.Strict);
        factoryMock.SetupGet(x => x.ClusterId).Returns(clusterId);
        factoryMock.Setup(x => x.GetComponent<IJobMasterSchedulerClusterAware>()).Returns(schedulerMock.Object);
        factoryMock.Setup(x => x.GetComponent<IMasterClusterConfigurationService>()).Returns(configServiceMock.Object);

        JobMasterClusterAwareComponentFactories.AddFactory(clusterId, factoryMock.Object);

        var ctx = JobMasterScheduler.Instance.Advanced.Recurring<TestDefinitionAttribute>(
            new NeverRecursCompiledExpr(),
            clusterId: clusterId);

        ctx.ClusterId.Should().Be(clusterId);
        scheduled.Should().NotBeNull();
        scheduled!.JobDefinitionId.Should().Be("advanced-defid");
        scheduled.Priority.Should().Be(JobMasterPriority.High);
        scheduled.Timeout.Should().Be(TimeSpan.FromSeconds(42));
        scheduled.WorkerLane.Should().Be("advanced-lane");
        schedulerMock.Verify();
    }

    [Fact]
    public void AdvancedRecurring_WithConfigObject_SchedulesUsingConfigValues()
    {
        var clusterId = "c-advanced-recurring-config";

        using var _ = new StaticStateScope(new FakeRuntime(started: true));

        JobMasterClusterConnectionConfig.Create(
            clusterId: clusterId,
            repositoryTypeId: "repo",
            connectionString: "cnn",
            isDefault: true);

        var configServiceMock = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);
        configServiceMock.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId));

        var schedulerMock = new Mock<IJobMasterSchedulerClusterAware>(MockBehavior.Strict);
        RecurringScheduleRawModel? scheduled = null;
        schedulerMock
            .Setup(x => x.Schedule(It.IsAny<RecurringScheduleRawModel>()))
            .Callback<RecurringScheduleRawModel>(m => scheduled = m)
            .Verifiable();

        var factoryMock = new Mock<IJobMasterClusterAwareComponentFactory>(MockBehavior.Strict);
        factoryMock.SetupGet(x => x.ClusterId).Returns(clusterId);
        factoryMock.Setup(x => x.GetComponent<IJobMasterSchedulerClusterAware>()).Returns(schedulerMock.Object);
        factoryMock.Setup(x => x.GetComponent<IMasterClusterConfigurationService>()).Returns(configServiceMock.Object);

        JobMasterClusterAwareComponentFactories.AddFactory(clusterId, factoryMock.Object);

        var config = new JobDefinitionConfig(
            "orders.recur",
            priority: JobMasterPriority.Low,
            timeout: TimeSpan.FromSeconds(15),
            workerLane: "orders");

        var ctx = JobMasterScheduler.Instance.Advanced.Recurring(config, new NeverRecursCompiledExpr(), clusterId: clusterId);

        ctx.ClusterId.Should().Be(clusterId);
        scheduled.Should().NotBeNull();
        scheduled!.JobDefinitionId.Should().Be("orders.recur");
        scheduled.Priority.Should().Be(JobMasterPriority.Low);
        scheduled.Timeout.Should().Be(TimeSpan.FromSeconds(15));
        scheduled.WorkerLane.Should().Be("orders");
        schedulerMock.Verify();
    }

    [Fact]
    public void AdvancedRecurring_WithConfigObject_WhenNoValuesSet_LeavesThemNullForReplanTimeResolution()
    {
        // Recurring schedules defer Priority/Timeout/MaxNumberOfRetries resolution to replan time
        // (RecurringSchedulePlanner), unlike one-time jobs which resolve eagerly — this is intentional so a
        // long-lived recurring schedule picks up config/attribute/cluster-default changes on its next
        // occurrence rather than freezing them in at creation time.
        var clusterId = "c-advanced-recurring-deferred";

        using var _ = new StaticStateScope(new FakeRuntime(started: true));

        JobMasterClusterConnectionConfig.Create(
            clusterId: clusterId,
            repositoryTypeId: "repo",
            connectionString: "cnn",
            isDefault: true);

        var configServiceMock = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);
        configServiceMock.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId));

        var schedulerMock = new Mock<IJobMasterSchedulerClusterAware>(MockBehavior.Strict);
        RecurringScheduleRawModel? scheduled = null;
        schedulerMock
            .Setup(x => x.Schedule(It.IsAny<RecurringScheduleRawModel>()))
            .Callback<RecurringScheduleRawModel>(m => scheduled = m)
            .Verifiable();

        var factoryMock = new Mock<IJobMasterClusterAwareComponentFactory>(MockBehavior.Strict);
        factoryMock.SetupGet(x => x.ClusterId).Returns(clusterId);
        factoryMock.Setup(x => x.GetComponent<IJobMasterSchedulerClusterAware>()).Returns(schedulerMock.Object);
        factoryMock.Setup(x => x.GetComponent<IMasterClusterConfigurationService>()).Returns(configServiceMock.Object);

        JobMasterClusterAwareComponentFactories.AddFactory(clusterId, factoryMock.Object);

        var config = new JobDefinitionConfig("orders.recur.bare");

        JobMasterScheduler.Instance.Advanced.Recurring(config, new NeverRecursCompiledExpr(), clusterId: clusterId);

        scheduled.Should().NotBeNull();
        scheduled!.Priority.Should().BeNull();
        scheduled.Timeout.Should().BeNull();
        scheduled.MaxNumberOfRetries.Should().BeNull();
    }

    private sealed class TestDefinitionAttribute : JobDefinitionConfigAttribute
    {
        public override JobDefinitionConfig Config { get; } = new JobDefinitionConfig(
            "advanced-defid",
            priority: JobMasterPriority.High,
            timeout: TimeSpan.FromSeconds(42),
            workerLane: "advanced-lane");
    }

    private sealed class TestJobMasterHandler : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    [JobMasterPriority(JobMasterPriority.Critical)]
    private sealed class CriticalPriorityHandler : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    private sealed class StaticStateScope : IDisposable
    {
        private readonly object? previousRuntime;

        private readonly IDictionary<string, IJobMasterClusterAwareComponentFactory>? factories;
        private readonly List<KeyValuePair<string, IJobMasterClusterAwareComponentFactory>> previousFactories;

        private readonly ISet<JobMasterClusterConnectionConfig>? clusterConfigs;
        private readonly List<JobMasterClusterConnectionConfig> previousClusterConfigs;

        private readonly object? previousDefaultBacking;

        public StaticStateScope(IJobMasterRuntime runtime)
        {
            var runtimeSingletonType = typeof(JobMasterRuntimeSingleton);
            var instanceField = runtimeSingletonType.GetField("_instance", BindingFlags.NonPublic | BindingFlags.Static);
            previousRuntime = instanceField?.GetValue(null);
            instanceField?.SetValue(null, runtime);

            var factoriesType = typeof(JobMasterClusterAwareComponentFactories);
            var factoriesField = factoriesType.GetField("factories", BindingFlags.NonPublic | BindingFlags.Static);
            factories = (IDictionary<string, IJobMasterClusterAwareComponentFactory>?)factoriesField?.GetValue(null);
            previousFactories = factories?.ToList() ?? new List<KeyValuePair<string, IJobMasterClusterAwareComponentFactory>>();
            factories?.Clear();

            var clusterCfgType = typeof(JobMasterClusterConnectionConfig);
            var clusterConfigsField = clusterCfgType.GetField("ClusterConfigs", BindingFlags.NonPublic | BindingFlags.Static);
            clusterConfigs = (ISet<JobMasterClusterConnectionConfig>?)clusterConfigsField?.GetValue(null);
            previousClusterConfigs = clusterConfigs?.ToList() ?? new List<JobMasterClusterConnectionConfig>();
            clusterConfigs?.Clear();

            var defaultBackingField = clusterCfgType.GetField("DefaultBacking", BindingFlags.NonPublic | BindingFlags.Static);
            previousDefaultBacking = defaultBackingField?.GetValue(null);
            defaultBackingField?.SetValue(null, null);
        }

        public void Dispose()
        {
            var runtimeSingletonType = typeof(JobMasterRuntimeSingleton);
            var instanceField = runtimeSingletonType.GetField("_instance", BindingFlags.NonPublic | BindingFlags.Static);
            instanceField?.SetValue(null, previousRuntime);

            factories?.Clear();
            foreach (var kvp in previousFactories)
            {
                factories![kvp.Key] = kvp.Value;
            }

            clusterConfigs?.Clear();
            foreach (var cfg in previousClusterConfigs)
            {
                clusterConfigs!.Add(cfg);
            }

            var clusterCfgType = typeof(JobMasterClusterConnectionConfig);
            var defaultBackingField = clusterCfgType.GetField("DefaultBacking", BindingFlags.NonPublic | BindingFlags.Static);
            defaultBackingField?.SetValue(null, previousDefaultBacking);
        }
    }
}