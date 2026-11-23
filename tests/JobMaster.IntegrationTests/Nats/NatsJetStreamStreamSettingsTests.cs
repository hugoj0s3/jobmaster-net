using FluentAssertions;
using JobMaster.NatsJetStream;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Master;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Testcontainers.Nats;
using Xunit;

namespace JobMaster.IntegrationTests.Nats;

[Trait("DB", "Nats")]
public sealed class NatsJetStreamStreamSettingsTests : IAsyncLifetime
{
    private readonly NatsContainer container = new NatsBuilder().WithImage("nats:2.10-alpine").Build();

    public Task InitializeAsync() => container.StartAsync();

    public Task DisposeAsync() => container.DisposeAsync().AsTask();

    [Fact]
    public async Task EnsureStream_NewStream_GetsTheDefaultMaxAgeAndReplicas()
    {
        var agentConnectionId = "it-nats-" + Guid.NewGuid().ToString("N");

        await NatsJetStreamConnector.EnsureStreamAsync(agentConnectionId, Config(), new FakeLogger());

        var stream = await GetStreamConfigAsync(agentConnectionId);
        stream.MaxAge.Should().Be(NatsJetStreamConstants.MaxAge);
        stream.NumReplicas.Should().Be(NatsJetStreamConstants.DefaultNumReplicas);
    }

    [Fact]
    public async Task EnsureStream_ExistingStreamWithoutMaxAge_IsUpdatedToTheConfiguredValue()
    {
        var agentConnectionId = "it-nats-" + Guid.NewGuid().ToString("N");
        var streamName = NatsJetStreamUtils.GetStreamName(agentConnectionId);

        // Created the way 0.0.11 did: no MaxAge, so messages were kept forever.
        await using (var nats = new NatsConnection(new NatsOpts { Url = container.GetConnectionString() }))
        {
            await new NatsJSContext(nats).CreateStreamAsync(new StreamConfig(streamName, new[] { $"{NatsJetStreamConstants.Prefix}{agentConnectionId}.>" }));
        }

        var logger = new FakeLogger();
        var config = Config();
        config.AdditionalConnConfig.SetValue(NatsJetStreamConfigKey.NamespaceUniqueKey, NatsJetStreamConfigKey.StreamMaxAgeKey, TimeSpan.FromDays(7));

        await NatsJetStreamConnector.EnsureStreamAsync(agentConnectionId, config, logger);

        (await GetStreamConfigAsync(agentConnectionId)).MaxAge.Should().Be(TimeSpan.FromDays(7));
        logger.Messages.Should().ContainSingle(m => m.Contains(streamName) && m.Contains("MaxAge"));
    }

    private JobMasterAgentProviderConfig Config() =>
        new(NatsJetStreamConstants.RepositoryTypeId, container.GetConnectionString());

    private async Task<StreamConfig> GetStreamConfigAsync(string agentConnectionId)
    {
        await using var nats = new NatsConnection(new NatsOpts { Url = container.GetConnectionString() });
        var stream = await new NatsJSContext(nats).GetStreamAsync(NatsJetStreamUtils.GetStreamName(agentConnectionId));
        return stream.Info.Config;
    }

    private sealed class FakeLogger : IJobMasterLogger
    {
        public List<string> Messages { get; } = new();

        public JobMasterClusterConnectionConfig ClusterConnConfig => null!;

        public void Log(JobMasterLogLevel level, string message, JobMasterLogCategory? category = null, string? referenceId = null,
            Exception? exception = null, string? sourceMember = null, string? sourceFile = null, int? sourceLine = null)
            => Messages.Add(message);

        public Task<List<LogItem>> QueryAsync(LogItemQueryCriteria criteria) => Task.FromResult(new List<LogItem>());

        public Task<int> CountAsync(LogItemQueryCriteria criteria) => Task.FromResult(0);

        public Task<LogItem?> GetAsync(Guid id) => Task.FromResult<LogItem?>(null);
    }
}
