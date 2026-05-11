using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils;
using Moq;

namespace JobMaster.UnitTests.Background.Runners;

internal sealed record RunnerFixture(
    Mock<IJobMasterBackgroundAgentWorker> Worker,
    RunnerFakes.FakeDistributedLocker Locker,
    RunnerFakes.FakeAgentWorkersService AgentWorkers,
    RunnerFakes.FakeJobsService JobsService,
    RunnerFakes.FakeBucketsService Buckets,
    RunnerFakes.FakeClusterConfigService ClusterConfig,
    RunnerFakes.FakeJobsRepository JobsRepository,
    RunnerFakes.FakeGenericRecordRepository GenericRecords,
    RunnerFakes.FakeRecurringSchedulesRepository RecurringSchedules,
    RunnerFakes.FakeAgentJobsDispatcherService JobsDispatcher,
    RunnerFakes.FakeHeartbeatService HeartbeatService,
    RunnerFakes.FakeHostService HostService,
    RunnerFakes.FakeAgentConnectionService AgentConnectionService,
    RunnerFakes.FakeMasterRecurringSchedulesService RecurringSchedulesService,
    RunnerFakes.FakeRecurringSchedulePlanner RecurringSchedulePlanner,
    RunnerFakes.FakeRecentlyInsertedRecurringScheduleQueue RecentlyInsertedQueue,
    Mock<IWorkerClusterOperations> WorkerClusterOps,
    string WorkerId,
    string ClusterId)
{
    public static RunnerFixture Create()
    {
        var clusterId = $"c{JobMasterRandomUtil.NewGuid4():N}";
        var workerId = $"w{JobMasterRandomUtil.NewGuid4():N}";
        var agentConnectionId = new AgentConnectionId(clusterId, "fake-agent");
        var hostId = new HostId(clusterId, "fake-host");
        var clusterConnConfig = JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: true);
        clusterConnConfig.MarkAsReady();

        // ── fakes ──────────────────────────────────────────────────────────────
        var locker = new RunnerFakes.FakeDistributedLocker();
        var agentWorkers = new RunnerFakes.FakeAgentWorkersService();
        var jobsService = new RunnerFakes.FakeJobsService();
        var buckets = new RunnerFakes.FakeBucketsService();
        var clusterConfig = new RunnerFakes.FakeClusterConfigService();
        var jobsRepository = new RunnerFakes.FakeJobsRepository();
        var genericRecords = new RunnerFakes.FakeGenericRecordRepository();
        var recurringSchedules = new RunnerFakes.FakeRecurringSchedulesRepository();
        var jobsDispatcher = new RunnerFakes.FakeAgentJobsDispatcherService();
        var heartbeatService = new RunnerFakes.FakeHeartbeatService();
        var hostService = new RunnerFakes.FakeHostService();
        var agentConnectionService = new RunnerFakes.FakeAgentConnectionService();
        var recurringSchedulesService = new RunnerFakes.FakeMasterRecurringSchedulesService();
        var recurringSchedulePlanner = new RunnerFakes.FakeRecurringSchedulePlanner();
        var recentlyInsertedQueue = new RunnerFakes.FakeRecentlyInsertedRecurringScheduleQueue();

        // ── mocks ──────────────────────────────────────────────────────────────
        var workerClusterOps = new Mock<IWorkerClusterOperations>(MockBehavior.Loose);
        var logger = new Mock<IJobMasterLogger>(MockBehavior.Loose);

        workerClusterOps
            .Setup(x => x.CountActiveCoordinatorWorkersAsync())
            .ReturnsAsync(1);

        workerClusterOps
            .Setup(x => x.ExecWithRetryAsync(
                It.IsAny<Action<IWorkerClusterOperations>>(),
                It.IsAny<int>(),
                It.IsAny<int>()))
            .Callback<Action<IWorkerClusterOperations>, int, int>((action, _, _) => action(workerClusterOps.Object))
            .Returns(Task.CompletedTask);

        workerClusterOps
            .Setup(x => x.ExecWithRetryAsync(
                It.IsAny<Func<IWorkerClusterOperations, Task>>(),
                It.IsAny<int>(),
                It.IsAny<int>()))
            .Returns<Func<IWorkerClusterOperations, Task>, int, int>(
                (func, _, _) => func(workerClusterOps.Object));

        // ── background worker ──────────────────────────────────────────────────
        var worker = new Mock<IJobMasterBackgroundAgentWorker>(MockBehavior.Loose);
        worker.SetupGet(x => x.ClusterConnConfig).Returns(clusterConnConfig);
        worker.SetupGet(x => x.AgentWorkerId).Returns(workerId);
        worker.SetupGet(x => x.AgentConnectionId).Returns(agentConnectionId);
        worker.SetupGet(x => x.HostId).Returns(hostId);
        worker.SetupGet(x => x.TransferBatchSize).Returns(50);
        worker.SetupGet(x => x.BucketBufferSize).Returns(10);
        worker.SetupGet(x => x.StopRequested).Returns(false);
        worker.SetupGet(x => x.StopImmediatelyRequested).Returns(false);
        worker.SetupGet(x => x.StopRequestedAt).Returns((DateTime?)null);
        worker.SetupGet(x => x.StopGracePeriod).Returns((TimeSpan?)null);
        worker.SetupGet(x => x.CancellationTokenSource).Returns(new CancellationTokenSource());
        worker.SetupGet(x => x.BucketAwareSemaphoreSlim).Returns(new SemaphoreSlim(1, 1));
        worker.SetupGet(x => x.MainSemaphoreSlim).Returns(new SemaphoreSlim(1, 1));
        worker.SetupGet(x => x.Runners).Returns(new List<IJobMasterRunner>());
        worker.SetupGet(x => x.WorkerClusterOperations).Returns(workerClusterOps.Object);
        worker.SetupGet(x => x.IsInitialized).Returns(true);

        // services
        worker.Setup(x => x.GetClusterAwareService<IMasterDistributedLockerService>()).Returns(locker);
        worker.Setup(x => x.GetClusterAwareService<IMasterAgentWorkersService>()).Returns(agentWorkers);
        worker.Setup(x => x.GetClusterAwareService<IMasterJobsService>()).Returns(jobsService);
        worker.Setup(x => x.GetClusterAwareService<IMasterBucketsService>()).Returns(buckets);
        worker.Setup(x => x.GetClusterAwareService<IMasterClusterConfigurationService>()).Returns(clusterConfig);
        worker.Setup(x => x.GetClusterAwareService<IAgentJobsDispatcherService>()).Returns(jobsDispatcher);
        worker.Setup(x => x.GetClusterAwareService<IWorkerClusterOperations>()).Returns(workerClusterOps.Object);
        worker.Setup(x => x.GetClusterAwareService<IJobMasterLogger>()).Returns(logger.Object);
        worker.Setup(x => x.GetClusterAwareService<IMasterHeartbeatService>()).Returns(heartbeatService);
        worker.Setup(x => x.GetClusterAwareService<IMasterHostService>()).Returns(hostService);
        worker.Setup(x => x.GetClusterAwareService<IMasterAgentConnectionService>()).Returns(agentConnectionService);
        worker.Setup(x => x.GetClusterAwareService<IMasterRecurringSchedulesService>()).Returns(recurringSchedulesService);
        worker.Setup(x => x.GetClusterAwareService<IRecurringSchedulePlanner>()).Returns(recurringSchedulePlanner);
        worker.Setup(x => x.GetClusterAwareService<IRecentlyInsertedRecurringScheduleQueue>()).Returns(recentlyInsertedQueue);

        // repositories
        worker.Setup(x => x.GetClusterAwareRepository<IMasterJobsRepository>()).Returns(jobsRepository);
        worker.Setup(x => x.GetClusterAwareRepository<IMasterGenericRecordRepository>()).Returns(genericRecords);
        worker.Setup(x => x.GetClusterAwareRepository<IMasterRecurringSchedulesRepository>()).Returns(recurringSchedules);

        return new RunnerFixture(
            worker,
            locker,
            agentWorkers,
            jobsService,
            buckets,
            clusterConfig,
            jobsRepository,
            genericRecords,
            recurringSchedules,
            jobsDispatcher,
            heartbeatService,
            hostService,
            agentConnectionService,
            recurringSchedulesService,
            recurringSchedulePlanner,
            recentlyInsertedQueue,
            workerClusterOps,
            workerId,
            clusterId);
    }

    // ── bucket helpers ─────────────────────────────────────────────────────────

    public static BucketModel ActiveBucket(string clusterId, string bucketId, string workerId = "fake-worker")
        => new(clusterId)
        {
            Id = bucketId,
            Status = BucketStatus.Active,
            AgentConnectionId = new AgentConnectionId(clusterId, "fake-agent"),
            AgentWorkerId = workerId,
            LastStatusChangeAt = DateTime.UtcNow.AddSeconds(-30),
        };

    public static BucketModel CompletingBucket(string clusterId, string bucketId, string workerId = "fake-worker")
        => new(clusterId)
        {
            Id = bucketId,
            Status = BucketStatus.Completing,
            AgentConnectionId = new AgentConnectionId(clusterId, "fake-agent"),
            AgentWorkerId = workerId,
            LastStatusChangeAt = DateTime.UtcNow.AddSeconds(-30),
        };

    public static BucketModel LostBucket(string clusterId, string bucketId, string workerId = "fake-worker")
        => new(clusterId)
        {
            Id = bucketId,
            Status = BucketStatus.Lost,
            AgentConnectionId = new AgentConnectionId(clusterId, "fake-agent"),
            AgentWorkerId = workerId,
            LastStatusChangeAt = DateTime.UtcNow.AddSeconds(-30),
        };

    public static BucketModel DrainingBucket(string clusterId, string bucketId, string workerId = "fake-worker")
        => new(clusterId)
        {
            Id = bucketId,
            Status = BucketStatus.Draining,
            AgentConnectionId = new AgentConnectionId(clusterId, "fake-agent"),
            AgentWorkerId = workerId,
            LastStatusChangeAt = DateTime.UtcNow.AddSeconds(-30),
        };

    public static BucketModel ReadyToDeleteBucket(string clusterId, string bucketId, string workerId = "fake-worker")
        => new(clusterId)
        {
            Id = bucketId,
            Status = BucketStatus.ReadyToDelete,
            AgentConnectionId = new AgentConnectionId(clusterId, "fake-agent"),
            AgentWorkerId = workerId,
            LastStatusChangeAt = DateTime.UtcNow.AddSeconds(-30),
        };

    // ── job helpers ────────────────────────────────────────────────────────────

    public static JobRawModel FinalizedJob(DateTime finalizedAt, JobMasterJobStatus status = JobMasterJobStatus.Succeeded)
        => new()
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            Status = status,
            FinalizedAt = finalizedAt,
        };

    // ── connection helpers ─────────────────────────────────────────────────────

    public static AgentConnectionModel AliveAgentConnection(string clusterId, string name = "fake-agent")
        => new(clusterId)
        {
            Id = new AgentConnectionId(clusterId, name),
            LastHeartbeatAt = DateTime.UtcNow,
            FootprintCreatedAt = DateTime.UtcNow,
        };

    public static AgentConnectionModel DeadAgentConnection(
        string clusterId,
        string name = "dead-agent",
        bool protectChanges = false)
        => new(clusterId)
        {
            Id = new AgentConnectionId(clusterId, name),
            LastHeartbeatAt = DateTime.UtcNow.AddMinutes(-10),
            FootprintCreatedAt = DateTime.UtcNow.AddHours(-1),
            ProtectConnectionChanges = protectChanges,
        };

    // ── host helpers ───────────────────────────────────────────────────────────

    public static HostModel AliveHost(string clusterId, string hostName = "fake-host")
        => new(clusterId)
        {
            Id = new HostId(clusterId, hostName),
            LastHeartbeat = DateTime.UtcNow,
        };

    public static HostModel DeadHost(string clusterId, string hostName = "dead-host")
        => new(clusterId)
        {
            Id = new HostId(clusterId, hostName),
            LastHeartbeat = DateTime.UtcNow.AddMinutes(-10),
        };

    // ── recurring schedule helpers ─────────────────────────────────────────────

    public static RecurringScheduleRawModel ActiveSchedule(string clusterId)
        => new(clusterId)
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            Status = RecurringScheduleStatus.Active,
        };

    public static RecurringScheduleRawModel InactiveSchedule(string clusterId)
        => new(clusterId)
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            Status = RecurringScheduleStatus.Inactive,
        };

    public static RecurringScheduleRawModel CanceledScheduleWithJobCancellationPending(string clusterId)
        => new(clusterId)
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            Status = RecurringScheduleStatus.Canceled,
            IsJobCancellationPending = true,
        };
}
