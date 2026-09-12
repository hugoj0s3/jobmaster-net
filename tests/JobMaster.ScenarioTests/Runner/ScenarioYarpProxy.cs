using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using Yarp.ReverseProxy.Configuration;

namespace JobMaster.ScenarioTests.Runner;

internal sealed class DynamicProxyConfigProvider : IProxyConfigProvider
{
    private volatile ConfigHolder holder = new(Array.Empty<RouteConfig>(), Array.Empty<ClusterConfig>());

    public IProxyConfig GetConfig() => holder;

    public void Update(IReadOnlyList<RouteConfig> routes, IReadOnlyList<ClusterConfig> clusters)
    {
        var old = holder;
        holder = new ConfigHolder(routes, clusters);
        old.SignalChange();
    }

    private sealed class ConfigHolder : IProxyConfig
    {
        private readonly CancellationTokenSource cts = new();

        public ConfigHolder(IReadOnlyList<RouteConfig> routes, IReadOnlyList<ClusterConfig> clusters)
        {
            Routes = routes;
            Clusters = clusters;
            ChangeToken = new CancellationChangeToken(cts.Token);
        }

        public IReadOnlyList<RouteConfig> Routes { get; }
        public IReadOnlyList<ClusterConfig> Clusters { get; }
        public IChangeToken ChangeToken { get; }

        public void SignalChange() => cts.Cancel();
    }
}

/// <summary>
/// In-process YARP reverse proxy that round-robins /schedule calls across every currently
/// registered TargetTestScheduleApp container. Backends are updated dynamically as containers
/// start/stop, without restarting the proxy itself.
/// </summary>
public sealed class ScenarioYarpProxy : IAsyncDisposable
{
    private readonly Dictionary<string, string> backends = new();

    private WebApplication? app;
    private DynamicProxyConfigProvider? configProvider;

    public string ProxyUrl { get; private set; } = "";

    public async Task StartAsync(CancellationToken ct = default)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();

        configProvider = new DynamicProxyConfigProvider();
        builder.Services.AddSingleton<IProxyConfigProvider>(configProvider);
        builder.Services.AddReverseProxy();

        app = builder.Build();
        app.MapReverseProxy();

        await app.StartAsync(ct);

        var server = app.Services.GetRequiredService<IServer>();
        var addressesFeature = server.Features.Get<IServerAddressesFeature>()
            ?? throw new InvalidOperationException("Could not resolve the proxy's bound address.");

        ProxyUrl = addressesFeature.Addresses.First();
    }

    public void AddBackend(string name, string baseUrl)
    {
        backends[name] = baseUrl;
        Republish();
    }

    public void RemoveBackend(string name)
    {
        backends.Remove(name);
        Republish();
    }

    private void Republish()
    {
        var destinations = backends.ToDictionary(
            kv => kv.Key,
            kv => new DestinationConfig { Address = kv.Value });

        var cluster = new ClusterConfig
        {
            ClusterId = "schedule-apps",
            Destinations = destinations
        };

        var route = new RouteConfig
        {
            RouteId = "schedule-route",
            ClusterId = "schedule-apps",
            Match = new RouteMatch { Path = "{**catch-all}" }
        };

        configProvider!.Update(new[] { route }, new[] { cluster });
    }

    public async ValueTask DisposeAsync()
    {
        if (app != null)
        {
            await app.StopAsync();
            await app.DisposeAsync();
        }
    }
}
