using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using JobMaster.MySql;
using JobMaster.Postgres;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc;
using JobMaster.Sdk.Repositories;
using JobMaster.Sdk.Utils;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.Hybrid;

// Hybrid agent connection against real stores: Postgres transport, MySQL execution (RepoConformanceBootstrap).
[Collection("RepoConformance")]
[Trait("DB", "Hybrid")]
public sealed class HybridAgentConnectionIntegrationTests : IAsyncLifetime
{
    private readonly RepoConformanceBootstrap bootstrap;

    private IJobMasterClusterAwareComponentFactory factory = null!;
    private JobMasterAgentConnectionConfig agentConfig = null!;
    private AgentConnectionId agentConnectionId = null!;

    public HybridAgentConnectionIntegrationTests(RepoConformanceBootstrap bootstrap)
    {
        this.bootstrap = bootstrap;
    }

    public async Task InitializeAsync()
    {
        await bootstrap.EnsureStartedAsync();

        factory = JobMasterClusterAwareComponentFactories.GetFactory(RepoConformanceBootstrap.HybridClusterId);
        agentConfig = JobMasterClusterConnectionConfig
            .Get(RepoConformanceBootstrap.HybridClusterId, includeNotReady: true)
            .GetAgentConnectionConfig(RepoConformanceBootstrap.HybridAgentConnectionName);
        agentConnectionId = new AgentConnectionId(agentConfig.Id);
    }

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Dispatcher_SavePendingGoesToTransport_ProcessingGoesToExecution()
    {
        var dispatcher = factory.GetComponent<IAgentComponentFactory>().GetRepository(agentConnectionId);
        var bucketId = "hyb-route-" + JobMasterRandomUtil.NewGuid4().ToString("N");
        await dispatcher.CreateBucketAsync(bucketId);

        try
        {
            var savePendingJob = NewJob(bucketId);
            var processingJob = NewJob(bucketId);
            await dispatcher.PushSavePendingJobAsync(savePendingJob);
            await dispatcher.PushForProcessingAsync(processingJob);

            var savePendingAddress = FullBucketAddressIdsUtil.GetJobSavePendingBucketAddress(bucketId);
            var processingAddress = FullBucketAddressIdsUtil.GetJobProcessingBucketAddress(bucketId);

            var transport = TransportRaw();
            var execution = ExecutionRaw();

            // Roles never swap: each message is only in its own store.
            (await execution.HasJobsAsync(savePendingAddress)).Should().BeFalse();
            (await transport.HasJobsAsync(processingAddress)).Should().BeFalse();

            var fromTransport = await transport.PullMessagesAsync(savePendingAddress, 10);
            fromTransport.Select(m => m.CorrelationId).Should().Equal(savePendingJob.Id.ToString("N"));

            var fromExecution = await execution.PullMessagesAsync(processingAddress, 10);
            fromExecution.Select(m => m.CorrelationId).Should().Equal(processingJob.Id.ToString("N"));
        }
        finally
        {
            await dispatcher.DestroyBucketAsync(bucketId);
        }
    }

    [Fact]
    public async Task BucketOperations_CoverBothStores()
    {
        var service = factory.GetService<IAgentJobsDispatcherService>();
        var dispatcher = factory.GetComponent<IAgentComponentFactory>().GetRepository(agentConnectionId);
        var bucketId = "hyb-bucket-" + JobMasterRandomUtil.NewGuid4().ToString("N");

        await service.CreateBucketAsync(agentConnectionId, bucketId);
        try
        {
            (await service.HasJobsAsync(agentConnectionId, bucketId)).Should().BeFalse();

            // Only the execution store has a message.
            await dispatcher.PushForProcessingAsync(NewJob(bucketId));
            (await service.HasJobsAsync(agentConnectionId, bucketId)).Should().BeTrue();
            await ExecutionRaw().PullMessagesAsync(FullBucketAddressIdsUtil.GetJobProcessingBucketAddress(bucketId), 10);
            (await service.HasJobsAsync(agentConnectionId, bucketId)).Should().BeFalse();

            // Only the transport store has a message.
            await dispatcher.PushSavePendingJobAsync(NewJob(bucketId));
            (await service.HasJobsAsync(agentConnectionId, bucketId)).Should().BeTrue();
        }
        finally
        {
            await service.DestroyBucketAsync(agentConnectionId, bucketId);
        }

        (await service.HasJobsAsync(agentConnectionId, bucketId)).Should().BeFalse();
    }

    [Fact]
    public async Task Fingerprint_IsComposedFromBothStores_StableAndStoredOnMaster()
    {
        var clusterId = RepoConformanceBootstrap.HybridClusterId;

        var transportFp = await NewRawFingerprintResolver(PostgresRepositoryConstants.RepositoryTypeId, agentConfig.TransportConfig)
            .GiveYourFingerprintAsync(clusterId, agentConfig.Id);
        var executionFp = await NewRawFingerprintResolver(MySqlRepositoryConstants.RepositoryTypeId, agentConfig.ExecutionConfig)
            .GiveYourFingerprintAsync(clusterId, agentConfig.Id);

        var resolver = factory.GetFingerprintResolver(agentConfig.RepositoryTypeId);
        resolver.Initialize(agentConfig);
        var hybridFp = await resolver.GiveYourFingerprintAsync(clusterId, agentConfig.Id);

        hybridFp.Should().Be(RepoTypeAndFingerprintUtil.FormatHybridFingerprint(transportFp, executionFp));

        // What startup saved on master must match, or ProtectConnectionChanges would trip on restart.
        var stored = await factory.GetComponent<IMasterAgentConnectionService>()
            .GetConnectionAsync(agentConnectionId, useCache: false);
        stored.Should().NotBeNull();
        stored!.RepositoryTypeId.Should().Be("hybrid:Postgres&&MySql");
        stored.Fingerprint.Should().Be(hybridFp);
    }

    private IAgentRawMessagesDispatcherRepository TransportRaw()
    {
        var raw = factory.ClusterServiceProvider.GetRequiredKeyedService<IAgentRawMessagesDispatcherRepository>(
            ClusterServiceKeys.GetAgentRawJobsDispatcherSavingKey(PostgresRepositoryConstants.RepositoryTypeId));
        raw.Initialize(agentConfig.Id, agentConfig.TransportConfig);
        return raw;
    }

    private IAgentRawMessagesDispatcherRepository ExecutionRaw()
    {
        var raw = factory.ClusterServiceProvider.GetRequiredKeyedService<IAgentRawMessagesDispatcherRepository>(
            ClusterServiceKeys.GetAgentRawJobsDispatcherProcessingKey(MySqlRepositoryConstants.RepositoryTypeId));
        raw.Initialize(agentConfig.Id, agentConfig.ExecutionConfig);
        return raw;
    }

    private IAgentRawFingerprintResolver NewRawFingerprintResolver(string repoType, JobMasterAgentProviderConfig providerConfig)
    {
        var resolver = factory.ClusterServiceProvider.GetAgentRawFingerprintResolver(repoType);
        resolver.Initialize(agentConfig.Id, providerConfig);
        return resolver;
    }

    private JobRawModel NewJob(string bucketId) => new(RepoConformanceBootstrap.HybridClusterId)
    {
        Id = JobMasterRandomUtil.NewGuid4(),
        JobDefinitionId = "hybrid-def",
        NextPlanExecutionAt = DateTime.UtcNow.AddSeconds(-1),
        ScheduledAt = DateTime.UtcNow,
        Priority = JobMasterPriority.Medium,
        Status = JobMasterJobStatus.OnMaster,
        Timeout = TimeSpan.FromSeconds(30),
        MaxNumberOfRetries = 0,
        AgentConnectionId = agentConnectionId,
        BucketId = bucketId,
    };
}
