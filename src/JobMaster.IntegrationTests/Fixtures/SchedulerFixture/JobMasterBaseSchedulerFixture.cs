using System.Collections.Concurrent;
using System.Diagnostics;
using JobMaster.Contracts.Models;
using JobMaster.IntegrationTests.Utils;
using JobMaster.Ioc.Extensions;
using JobMaster.MySql;
using JobMaster.Postgres;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Services.Master;
using JobMaster.Sql;
using JobMaster.SqlServer;
using JobMaster.NatJetStreams;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.IntegrationTests.Fixtures.SchedulerFixture;

public abstract class JobMasterBaseSchedulerFixture : IAsyncLifetime
{
    private enum DbProvider
    {
        Postgres,
        MySql,
        SqlServer,
        NatJetStream
    }

    private sealed class TestClusterDefinition
    {
        public string ClusterName { get; set; } = string.Empty;
        public DbProvider DbProvider { get; set; }
        public string ConnectionString { get; set; } = string.Empty;
        public string? MasterTablePrefix { get; set; }
        public bool IsDefault { get; set; }
        public List<TestAgentConnectionDefinition> AgentConnections { get; set; } = new();
    }

    private sealed class TestAgentConnectionDefinition
    {
        public string AgentName { get; set; } = string.Empty;
        public DbProvider DbProvider { get; set; }
        public string ConnectionString { get; set; } = string.Empty;
        public string? AgentTablePrefix { get; set; }
        public List<TestWorkerDefinition> Workers { get; set; } = new();
    }

    private sealed class TestWorkerDefinition
    {
        public string? WorkerLane { get; set; }
        public int BucketQty { get; set; } = 3;
    }

    public IServiceProvider Services { get; private set; } = null!;
    public bool IsConfigured { get; private set; }
    public string? NotConfiguredReason { get; private set; }
    
    public IList<string> ClusterIds { get; private set; } = new List<string>();
    public IList<string> WorkerLanes { get; private set; } = new List<string>();
    
    public ConcurrentDictionary<string, List<LogItem>> Dictionarylogs = new(StringComparer.OrdinalIgnoreCase);
    
    // Abstract filter/default settings to be provided by concrete fixtures
    public abstract string IncludeWildcards { get; }
    public abstract string ExcludeWildcards { get; }
    public abstract string DefaultClusterId { get; }
    public virtual bool IsDrainingModeTest => false;
    
    public async Task InitializeAsync()
    {
        Trace.Listeners.Clear();
        Trace.AutoFlush = true;
        Trace.Listeners.Add(new TextWriterTraceListener(Path.Combine(AppContext.BaseDirectory, "integration-tests-trace.log")));

        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables()
            .Build();

        var workerLanes = config
            .GetSection("JobMaster:IntegrationTests:WorkerLanes")
            .Get<string[]>()
            ?? Array.Empty<string>();
        
        JobMasterLogger.OnLog = OnLog;
        this.WorkerLanes = workerLanes.ToList();
        
        var services = new ServiceCollection();

        var clusterDefs = config
                              .GetSection("JobMaster:IntegrationTests:Clusters")
                              .Get<List<TestClusterDefinition>>()
                          ?? new List<TestClusterDefinition>();

        // Apply cluster name filtering using fixture-provided wildcards
        var includePatterns = ParseWildcards(IncludeWildcards);
        var excludePatterns = ParseWildcards(ExcludeWildcards);
        if (includePatterns.Count > 0 || excludePatterns.Count > 0)
        {
            clusterDefs = clusterDefs
                .Where(c => (includePatterns.Count == 0 || IsMatchAny(c.ClusterName, includePatterns))
                            && (excludePatterns.Count == 0 || !IsMatchAny(c.ClusterName, excludePatterns)))
                .ToList();
        }

        // Override default cluster if provided by fixture
        var defaultClusterFromFixture = DefaultClusterId;
        if (!string.IsNullOrWhiteSpace(defaultClusterFromFixture))
        {
            foreach (var c in clusterDefs)
            {
                c.IsDefault = string.Equals(c.ClusterName, defaultClusterFromFixture, StringComparison.OrdinalIgnoreCase);
            }
        }

        CreateClustersFromDefinitions(services, clusterDefs);

        await DropTablesFromDefinitions(clusterDefs);
        

        IsConfigured = ClusterIds.Count > 0;
        NotConfiguredReason = IsConfigured
            ? null
            : "Integration tests require at least one cluster configured. Either set JobMaster:IntegrationTests:Clusters (preferred) or set one of: JobMaster:IntegrationTests:MasterPostgres/AgentsPostgres, MasterMySql/AgentsMySql, or MasterSqlServer/AgentsSqlServer in appsettings.json (or via env vars).";

        if (!IsConfigured)
        {
            Services = services.BuildServiceProvider();
            return;
        }


        Services = services.BuildServiceProvider();

        // Start runtime (runs runtime setups, provisions schema, starts workers).
        await Services.StartJobMasterRuntimeAsync();
        
        await Task.Delay(TimeSpan.FromSeconds(5));
    }

    private void CreateClustersFromDefinitions(ServiceCollection services, IReadOnlyList<TestClusterDefinition> clusterDefs)
    {
        foreach (var c in clusterDefs)
        {
            if (string.IsNullOrWhiteSpace(c.ClusterName) || string.IsNullOrWhiteSpace(c.ConnectionString))
            {
                continue;
            }

            services.AddJobMasterCluster(c.ClusterName, cfg =>
            {
                // if (this.IsDrainingModeTest)
                // {
                //     cfg.ClusterDbOperationThrottleLimit(25);
                // }
                
                switch (c.DbProvider)
                {
                    case DbProvider.Postgres:
                        cfg.UsePostgresForMaster(c.ConnectionString);
                        break;
                    case DbProvider.MySql:
                        cfg.UseMySqlForMaster(c.ConnectionString);
                        break;
                    case DbProvider.SqlServer:
                        cfg.UseSqlServerForMaster(c.ConnectionString);
                        break;
                }

                if (c.IsDefault)
                {
                    cfg.SetAsDefault();
                }

                if (!string.IsNullOrWhiteSpace(c.MasterTablePrefix))
                {
                    cfg.UseSqlTablePrefixForMaster(ToSafeSqlIdentifier(c.MasterTablePrefix));
                }

                cfg.ClusterTransientThreshold(TimeSpan.FromMinutes(1));

               // cfg.ClusterDbOperationThrottleLimit(10);

                var defaultAgentTablePrefix = !string.IsNullOrWhiteSpace(c.MasterTablePrefix)
                    ? ToSafeSqlIdentifier($"{c.ClusterName}{c.MasterTablePrefix}")
                    : null;

                foreach (var a in c.AgentConnections)
                {
                    if (string.IsNullOrWhiteSpace(a.AgentName) || string.IsNullOrWhiteSpace(a.ConnectionString))
                    {
                        continue;
                    }

                    var agentCfg = cfg.AddAgentConnectionConfig(a.AgentName);
                    // if (IsDrainingModeTest)
                    // {
                    //     agentCfg.AgentDbOperationThrottleLimit(25);
                    // }
                    
                    
                    switch (a.DbProvider)
                    {
                        case DbProvider.Postgres:
                            agentCfg.UsePostgresForAgent(a.ConnectionString);
                            break;
                        case DbProvider.MySql:
                            agentCfg.UseMySqlForAgent(a.ConnectionString);
                            break;
                        case DbProvider.SqlServer:
                            agentCfg.UseSqlServerForAgent(a.ConnectionString);
                            break;
                        case DbProvider.NatJetStream:
                            agentCfg.UseNatJetStream(a.ConnectionString);
                            break;
                    }

                    if (!string.IsNullOrWhiteSpace(a.AgentTablePrefix))
                    {
                        agentCfg.UseSqlTablePrefixForAgent(ToSafeSqlIdentifier(a.AgentTablePrefix));
                    }
                    else
                    {
                        agentCfg.UseSqlTablePrefixForAgent(ToSafeSqlIdentifier(defaultAgentTablePrefix ?? a.AgentName));
                    }

                    var workers = a.Workers;
                    foreach (var w in workers)
                    {
                        var selector = cfg.AddWorker().AgentConnName(a.AgentName);
                            
                        if (!string.IsNullOrWhiteSpace(w.WorkerLane))
                        {
                            selector.WorkerLane(w.WorkerLane);
                        }
                            
                        selector.BucketQtyConfig(JobMasterPriority.VeryLow, w.BucketQty)
                            .BucketQtyConfig(JobMasterPriority.Low, w.BucketQty)
                            .BucketQtyConfig(JobMasterPriority.Medium, w.BucketQty)
                            .BucketQtyConfig(JobMasterPriority.High, w.BucketQty)
                            .BucketQtyConfig(JobMasterPriority.Critical, w.BucketQty)
                            .ParallelismFactor(2)
                            .WorkerBatchSize(1000)
                            .SkipWarmUpTime();
                    }

                    if (IsDrainingModeTest)
                    {
                        var drainModeSelector = cfg.AddWorker().AgentConnName(a.AgentName);
                        drainModeSelector
                            .SetWorkerMode(AgentWorkerMode.Drain)
                            .WorkerBatchSize(1000)
                            .SkipWarmUpTime();
                        
                        var coordinatorModeSelector = cfg.AddWorker().AgentConnName(a.AgentName);
                        coordinatorModeSelector
                            .SetWorkerMode(AgentWorkerMode.Coordinator)
                            .WorkerBatchSize(1000);
                    }
                }

                cfg.ClusterMode(ClusterMode.Active);
            });

            ClusterIds.Add(c.ClusterName);
        }
    }

    private static async Task DropTablesFromDefinitions(IReadOnlyList<TestClusterDefinition> clusterDefs)
    {
        foreach (var c in clusterDefs)
        {
            var masterPrefix = ToSafeSqlIdentifier(c.MasterTablePrefix ?? string.Empty);
            var defaultAgentPrefix = !string.IsNullOrWhiteSpace(c.MasterTablePrefix)
                ? ToSafeSqlIdentifier($"{c.ClusterName}{c.MasterTablePrefix}")
                : null;
            if (!string.IsNullOrWhiteSpace(c.ConnectionString))
            {
                switch (c.DbProvider)
                {
                    case DbProvider.Postgres:
                        await PostgresTestDbUtil.DropMasterTablesAsync(c.ConnectionString, masterPrefix);
                        break;
                    case DbProvider.MySql:
                        await MySqlTestDbUtil.DropMasterTablesAsync(c.ConnectionString, masterPrefix);
                        break;
                    case DbProvider.SqlServer:
                        await SqlServerTestDbUtil.DropMasterTablesAsync(c.ConnectionString, masterPrefix);
                        break;
                }
            }

            foreach (var a in c.AgentConnections)
            {
                if (string.IsNullOrWhiteSpace(a.ConnectionString))
                {
                    continue;
                }

                var agentPrefix = ToSafeSqlIdentifier(a.AgentTablePrefix ?? defaultAgentPrefix ?? a.AgentName);
                switch (a.DbProvider)
                {
                    case DbProvider.Postgres:
                        await PostgresTestDbUtil.DropAgentTablesAsync(a.ConnectionString, agentPrefix);
                        break;
                    case DbProvider.MySql:
                        await MySqlTestDbUtil.DropAgentTablesAsync(a.ConnectionString, agentPrefix);
                        break;
                    case DbProvider.SqlServer:
                        await SqlServerTestDbUtil.DropAgentTablesAsync(a.ConnectionString, agentPrefix);
                        break;
                    case DbProvider.NatJetStream:
                        break;
                }
            }
        }
    }
    
    public Task DisposeAsync()
    {
        // Runtime currently has no async Stop API used here;
        // you can call StopImmediatelyAsync if you want to add it later.
        return Task.CompletedTask;
    }
    
    private void OnLog(string? clusterId, LogItem logItem)
    {
        var cid = clusterId ?? "default";
        var list = Dictionarylogs.GetOrAdd(cid, _ => new List<LogItem>());
    
        lock (list)
        {
            list.Add(logItem);
        }
    }

    private static string ToSafeSqlIdentifier(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        var chars = value.ToCharArray();
        for (var i = 0; i < chars.Length; i++)
        {
            var c = chars[i];
            if (char.IsLetterOrDigit(c) || c == '_')
            {
                continue;
            }

            chars[i] = '_';
        }

        return new string(chars);
    }

    private static IList<string> ParseWildcards(string? wildCards)
    {
        if (string.IsNullOrWhiteSpace(wildCards))
        {
            return Array.Empty<string>();
        }

        return wildCards
            .Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .ToList();
    }

    private static bool IsMatchAny(string value, IList<string> wildcardPatterns)
    {
        if (value.Contains("natjetstream"))
        {
            new object();
        }
        
        foreach (var wildcardPattern in wildcardPatterns)
        {
            var regexPattern = "^" + System.Text.RegularExpressions.Regex.Escape(wildcardPattern)
                .Replace("\\*", ".*")
                .Replace("\\?", ".") + "$";

            if (System.Text.RegularExpressions.Regex.IsMatch(value, regexPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.CultureInvariant))
            {
                return true;
            }
        }

        return false;
    }

    // Public helper for tests to filter cluster IDs using include/exclude wildcard patterns
    public static IList<string> FilterClusterIds(IList<string> clusterIds, string? includeWildCards, string? excludeWildCards)
    {
        var includes = ParseWildcards(includeWildCards);
        var excludes = ParseWildcards(excludeWildCards);

        return clusterIds
            .Where(id => includes.Count == 0 || IsMatchAny(id, includes))
            .Where(id => excludes.Count == 0 || !IsMatchAny(id, excludes))
            .ToList();
    }
}
