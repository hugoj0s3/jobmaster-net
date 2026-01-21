using System.Reflection;
using FluentAssertions;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Jobs;
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

        var ctx = JobMasterScheduler.Instance.OnceNow<TestJobHandler>();

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

        var ctx = await JobMasterScheduler.Instance.OnceNowAsync<TestJobHandler>();

        ctx.ClusterId.Should().Be(clusterId);
        schedulerMock.Verify();
        configServiceMock.Verify();
    }

    private sealed class TestJobHandler : IJobHandler
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