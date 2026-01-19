using JobMaster.NatJetStream;
using Microsoft.Extensions.Configuration;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace JobMaster.IntegrationTests.Fixtures.SchedulerFixture.NatJetStream;

public sealed class NatJetStreamDrainModeFixture : JobMasterBaseSchedulerFixture
{
    public override string IncludeWildcards => "*postgres-natjetstream";
    public override string ExcludeWildcards => "";
    public override string DefaultClusterId => "cluster-postgres-natjetstream";
    public override bool IsDrainingModeTest => true;

    public new async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await CleanupJetStreamAsync();
    }

    private static async Task CleanupJetStreamAsync()
    {
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
