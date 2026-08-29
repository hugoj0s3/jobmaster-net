using System.Text.Json;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background.Runners.JobsExecution;
using JobMaster.Sdk.Utils;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace JobMaster.UnitTests.Background;

internal sealed record EngineFixture(
    JobsExecutionEngine Engine,
    Mock<IWorkerClusterOperations> Ops,
    Mock<IMasterJobsService> Jobs,
    Mock<IMasterRecurringSchedulesService> Schedules,
    Mock<IMasterBucketsService> Buckets,
    Mock<IMasterClusterConfigurationService> ClusterCfg,
    FakeJobMasterHandler MasterHandler,
    string BucketId,
    string ClusterId,
    int BufferSize,
    IList<JobRawModel> BulkUpdateWatcher,
    IList<JobRawModel> SingleUpdateWatcher);

internal static class JobsExecutionEngineFixture
{
    public static EngineFixture Create(
        JobMasterPriority priority = JobMasterPriority.High,
        int bufferSize = 10)
    {
        var clusterId = $"c{JobMasterRandomUtil.NewGuid4():N}";
        var bucketId = $"b{JobMasterRandomUtil.NewGuid4():N}";
        var clusterConfig = JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: true);

        var ops = new Mock<IWorkerClusterOperations>(MockBehavior.Loose);
        var jobs = new Mock<IMasterJobsService>(MockBehavior.Loose);
        var schedules = new Mock<IMasterRecurringSchedulesService>(MockBehavior.Loose);
        var buckets = new Mock<IMasterBucketsService>(MockBehavior.Loose);
        var logger = new Mock<IJobMasterLogger>(MockBehavior.Loose);
        var locker = new Mock<IMasterDistributedLockerService>(MockBehavior.Loose);
        var clusterCfg = new Mock<IMasterClusterConfigurationService>(MockBehavior.Loose);

        // ── service provider chain for ExecuteJobAsync ────────────────────────
        var handler = new FakeJobMasterHandler();
        var innerSp = new Mock<IServiceProvider>();
        var serviceScope = new Mock<IServiceScope>();
        var scopeFactory = new Mock<IServiceScopeFactory>();
        var serviceProvider = new Mock<IServiceProvider>();

        innerSp.Setup(x => x.GetService(typeof(FakeJobMasterHandler))).Returns(handler);
        serviceScope.SetupGet(x => x.ServiceProvider).Returns(innerSp.Object);
        scopeFactory.Setup(x => x.CreateScope()).Returns(serviceScope.Object);
        serviceProvider.Setup(x => x.GetService(typeof(IServiceScopeFactory))).Returns(scopeFactory.Object);

        var worker = new Mock<IJobMasterBackgroundAgentWorker>(MockBehavior.Loose);
        worker.SetupGet(x => x.ClusterConnConfig).Returns(clusterConfig);
        worker.SetupGet(x => x.BucketBufferSize).Returns(bufferSize);
        worker.SetupGet(x => x.ParallelismFactor).Returns(1.0);
        worker.SetupGet(x => x.WorkerClusterOperations).Returns(ops.Object);
        worker.SetupGet(x => x.CancellationTokenSource).Returns(new CancellationTokenSource());
        worker.SetupGet(x => x.ServiceProvider).Returns(serviceProvider.Object);
        worker.Setup(x => x.GetClusterAwareService<IMasterJobsService>()).Returns(jobs.Object);
        worker.Setup(x => x.GetClusterAwareService<IMasterRecurringSchedulesService>()).Returns(schedules.Object);
        worker.Setup(x => x.GetClusterAwareService<IMasterBucketsService>()).Returns(buckets.Object);
        worker.Setup(x => x.GetClusterAwareService<IJobMasterLogger>()).Returns(logger.Object);
        worker.Setup(x => x.GetClusterAwareService<IMasterDistributedLockerService>()).Returns(locker.Object);
        worker.Setup(x => x.GetClusterAwareService<IMasterClusterConfigurationService>()).Returns(clusterCfg.Object);

        // ── watchers ──────────────────────────────────────────────────────────
        var bulkUpdateWatcher = new List<JobRawModel>();
        var singleUpdateWatcher = new List<JobRawModel>();

        jobs.Setup(x => x.QueryAsync(It.IsAny<JobQueryCriteria>()))
            .ReturnsAsync(new List<JobRawModel>());

        jobs.Setup(x => x.BulkUpdateAsync(It.IsAny<IList<JobRawModel>>(), It.IsAny<OperationThrottler>()))
            .Callback<IList<JobRawModel>, OperationThrottler>((list, _) =>
            {
                foreach (var job in list)
                    bulkUpdateWatcher.Add(Clone(job));
            })
            .ReturnsAsync((IList<JobRawModel> list, OperationThrottler _) => list.ToList());

        ops.Setup(x => x.UpdateAsync(It.IsAny<JobRawModel>(), It.IsAny<JobExecution?>(), It.IsAny<OperationThrottler>()))
            .Callback<JobRawModel, JobExecution?, OperationThrottler>((job, _, __) => singleUpdateWatcher.Add(Clone(job)));

        ops.Setup(x => x.Update(It.IsAny<JobRawModel>(), It.IsAny<JobExecution?>(), It.IsAny<OperationThrottler>()))
            .Callback<JobRawModel, JobExecution?, OperationThrottler>((job, _, __) => singleUpdateWatcher.Add(Clone(job)));

        ops.Setup(x => x.ExecWithRetryAsync(
                It.IsAny<Action<IWorkerClusterOperations>>(),
                It.IsAny<int>(),
                It.IsAny<int>()))
            .Callback<Action<IWorkerClusterOperations>, int, int>((action, _, __) => action(ops.Object))
            .Returns(Task.CompletedTask);

        ops.Setup(x => x.ExecWithRetryAsync(
                It.IsAny<Func<IWorkerClusterOperations, Task>>(),
                It.IsAny<int>(),
                It.IsAny<int>()))
            .Returns<Func<IWorkerClusterOperations, Task>, int, int>(
                (func, _, __) => func(ops.Object));

        ops.Setup(x => x.BulkUpdateAsync(It.IsAny<IList<JobRawModel>>()))
            .Returns<IList<JobRawModel>>(list => jobs.Object.BulkUpdateAsync(list));

        ops.Setup(x => x.BulkUpdateAsync(It.IsAny<BulkJobUpdateRequest>()))
            .Returns(Task.CompletedTask);

        ops.Setup(x => x.ExecWithRetryAsync(
                It.IsAny<Func<IWorkerClusterOperations, Task<IList<JobRawModel>>>>(),
                It.IsAny<int>(),
                It.IsAny<int>()))
            .Returns<Func<IWorkerClusterOperations, Task<IList<JobRawModel>>>, int, int>(
                (func, _, __) => func(ops.Object));

        var engine = new JobsExecutionEngine(worker.Object, bucketId, priority);
        return new EngineFixture(
            engine,
            ops,
            jobs,
            schedules,
            buckets,
            clusterCfg,
            handler,
            bucketId,
            clusterId,
            bufferSize,
            bulkUpdateWatcher,
            singleUpdateWatcher);
    }

    /// <summary>
    /// Returns a <see cref="ClusterConfigurationModel"/> set to Active for the given cluster.
    /// Assign to <c>f.ClusterCfg.Setup(x => x.Get()).Returns(...)</c> to enable
    /// real job execution in simulation tests.
    /// </summary>
    public static ClusterConfigurationModel ActiveClusterConfig(string clusterId)
        => new(clusterId) { ClusterMode = ClusterMode.Active };

    /// <summary>
    /// Creates an InBucket job with a past NextPlanExecutionAt so it passes the onboarding
    /// window check, and a far-future ProcessDeadline so it is not deadline-exceeded on arrival.
    /// </summary>
    public static JobRawModel CreateInBucketJob(
        DateTime? nextPlanExecutionAt = null,
        string jobDefinitionId = FakeJobMasterHandler.DefinitionId)
    {
        return new JobRawModel
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            Status = JobMasterJobStatus.InBucket,
            JobDefinitionId = jobDefinitionId,
            NextPlanExecutionAt = nextPlanExecutionAt ?? DateTime.UtcNow.AddMinutes(-1),
            ProcessDeadline = DateTime.UtcNow.AddMinutes(10),
            Timeout = TimeSpan.FromMinutes(5),
        };
    }

    /// <summary>
    /// Creates multiple InBucket jobs. Defaults to 50 — enough to cross the default
    /// repository-type MaxBatchSize (50, see <c>OperationThrottlerSettingsTemplateFactory</c>)
    /// partition boundary.
    /// </summary>
    public static IList<JobRawModel> CreateInBucketJobMany(
        DateTime? nextPlanExecutionAt = null,
        int count = 50,
        string jobDefinitionId = FakeJobMasterHandler.DefinitionId)
    {
        return new object[count].Select(_ => CreateInBucketJob(nextPlanExecutionAt, jobDefinitionId)).ToList();
    }

    /// <summary>
    /// Creates an InBucket job tied to a recurring schedule.
    /// </summary>
    public static JobRawModel CreateRecurringJob(
        Guid sourceId,
        JobMasterTriggerSourceType triggerType = JobMasterTriggerSourceType.StaticRecurring)
    {
        return new JobRawModel
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            Status = JobMasterJobStatus.InBucket,
            JobDefinitionId = FakeJobMasterHandler.DefinitionId,
            SourceId = sourceId,
            TriggerSourceType = triggerType,
            NextPlanExecutionAt = DateTime.UtcNow.AddMinutes(-1),
            ProcessDeadline = DateTime.UtcNow.AddMinutes(10),
            Timeout = TimeSpan.FromMinutes(5),
        };
    }

    /// <summary>
    /// Creates an InBucket job whose <see cref="JobRawModel.ProcessDeadline"/> is already in the
    /// past so it will be returned by a deadline query and picked up by
    /// <c>CheckDeadlineJobsAsync</c>.
    /// </summary>
    public static JobRawModel CreateDeadlineExceededJob()
    {
        return new JobRawModel
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            Status = JobMasterJobStatus.InBucket,
            JobDefinitionId = FakeJobMasterHandler.DefinitionId,
            NextPlanExecutionAt = DateTime.UtcNow.AddMinutes(-5),
            ProcessDeadline = DateTime.UtcNow.AddMinutes(-1),
            Timeout = TimeSpan.FromMinutes(5),
        };
    }

    public static BucketModel ActiveBucket(string clusterId, string bucketId)
        => new(clusterId) { Id = bucketId, Status = BucketStatus.Active };

    public static BucketModel CompletingBucket(string clusterId, string bucketId)
        => new(clusterId) { Id = bucketId, Status = BucketStatus.Completing };

    /// <summary>
    /// Returns a deep clone of <paramref name="job"/> by round-tripping through JSON,
    /// capturing the exact field values at the moment of the call.
    /// </summary>
    private static JobRawModel Clone(JobRawModel job)
        => JsonSerializer.Deserialize<JobRawModel>(JsonSerializer.Serialize(job))!;
}
