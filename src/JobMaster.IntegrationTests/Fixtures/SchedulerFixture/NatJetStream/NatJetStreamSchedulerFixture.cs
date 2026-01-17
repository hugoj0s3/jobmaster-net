using JobMaster.NatJetStreams;
using Microsoft.Extensions.Configuration;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace JobMaster.IntegrationTests.Fixtures.SchedulerFixture.NatJetStream;

public sealed class NatJetStreamSchedulerFixture : JobMasterBaseSchedulerFixture
{
    public override string IncludeWildcards => "*-postgres-natjetstream";
    public override string ExcludeWildcards => "";
    public override string DefaultClusterId => "cluster-postgres-natjetstream";

    public new async Task InitializeAsync()
    {
        await base.InitializeAsync();
        // base.InitializeAsync already builds Services and starts runtime; for a true pre-start cleanup
        // you'd override the base flow. To keep it minimal, perform a best-effort cleanup before tests run next time.
        await CleanupJetStreamAsync();
    }

    private static async Task CleanupJetStreamAsync()
    {
        // Load integration test config directly to identify NatJetStream agents
        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables()
            .Build();

        var clustersSection = config.GetSection("JobMaster:IntegrationTests:Clusters");
        foreach (var cluster in clustersSection.GetChildren())
        {
            var agents = cluster.GetSection("AgentConnections").GetChildren();
            foreach (var agent in agents)
            {
                var dbProvider = agent.GetValue<string>("DbProvider");
                if (!string.Equals(dbProvider, "NatJetStream", StringComparison.OrdinalIgnoreCase))
                    continue;

                var agentId = agent.GetValue<string>("AgentName") ?? string.Empty;
                var conn = agent.GetValue<string>("ConnectionString") ?? string.Empty;
                if (string.IsNullOrWhiteSpace(agentId) || string.IsNullOrWhiteSpace(conn))
                    continue;

                var streamName = NatJetStreamUtils.GetStreamName(agentId);
                try
                {
                    var opts = NatsOpts.Default with { Url = conn, Name = $"cleanup_{streamName}" };
                    await using var nats = new NatsConnection(opts);
                    await nats.ConnectAsync();
                    var js = new NatsJSContext(nats);    
                    await js.DeleteStreamAsync(streamName);
                }
                catch
                {
                    // ignore; stream may not exist or server may be down
                }
            }
        }
    }
}
