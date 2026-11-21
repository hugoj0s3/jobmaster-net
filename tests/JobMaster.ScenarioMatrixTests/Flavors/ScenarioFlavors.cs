using System.Text.Json.Nodes;

namespace JobMaster.ScenarioMatrixTests.Flavors;

/// <summary>
/// Every flavor a templated scenario can run on. Connection strings keep the runner's infrastructure tokens
/// (<c>{{PostgresHost}}</c>, ...), which it renders after the flavor's placeholders.
/// </summary>
public static class ScenarioFlavors
{
    public static readonly ScenarioFlavor PostgresPure = new(
        "PostgresPure", "pg", "Postgres", Postgres,
        (f, slot) => Single("Postgres", Postgres(f.DatabaseName($"Agent{slot}"))));

    public static readonly ScenarioFlavor MySqlPure = new(
        "MySqlPure", "my", "MySql", MySql,
        (f, slot) => Single("MySql", MySql(f.DatabaseName($"Agent{slot}"))));

    public static readonly ScenarioFlavor SqlServerPure = new(
        "SqlServerPure", "ss", "SqlServer", SqlServer,
        (f, slot) => Single("SqlServer", SqlServer(f.DatabaseName($"Agent{slot}"))));

    public static readonly ScenarioFlavor RavenDbPure = new(
        "RavenDbPure", "rv", "RavenDB", RavenDb,
        (f, slot) => Single("RavenDB", RavenDb(f.DatabaseName($"Agent{slot}"))));

    public static readonly ScenarioFlavor PostgresNats = new(
        "PostgresNats", "pgnats", "Postgres", Postgres,
        (_, _) => Single("NatsJetStream", Nats()));

    // Odd slots NATS → RavenDB, even slots RavenDB → NATS, so a scenario with 2+ slots covers both directions.
    public static readonly ScenarioFlavor HybridNatsRavenDb = new(
        "HybridNatsRavenDb", "hyb", "RavenDB", RavenDb,
        (f, slot) => slot % 2 == 1
            ? Hybrid("NatsJetStream", Nats(), "RavenDB", RavenDb(f.DatabaseName($"AgentExec{slot}")))
            : Hybrid("RavenDB", RavenDb(f.DatabaseName($"AgentTransport{slot}")), "NatsJetStream", Nats()));

    public static readonly IReadOnlyList<ScenarioFlavor> All =
        [PostgresPure, MySqlPure, SqlServerPure, RavenDbPure, PostgresNats, HybridNatsRavenDb];

    /// <summary>Exclusion reason for the agent-only flavors in a scenario whose clusters are all standalone.</summary>
    public const string StandaloneHasNoAgent = "standalone has no agent connection: same run as its master's Pure flavor";

    /// <summary>Exclusion reason for the agent-only flavors in a scenario whose clusters declare no agent connection.</summary>
    public const string NoAgentConnection = "no agent connection declared: same run as its master's Pure flavor";

    /// <summary>
    /// Every flavor except <paramref name="excluded"/>, for a scenario's theory data. Each exclusion needs a reason,
    /// so it's explicit and searchable; a new flavor runs on every scenario that doesn't exclude it.
    /// </summary>
    public static IReadOnlyList<ScenarioFlavor> AllExcept(params (ScenarioFlavor Flavor, string Reason)[] excluded)
    {
        foreach (var (flavor, reason) in excluded)
        {
            if (string.IsNullOrWhiteSpace(reason))
            {
                throw new ArgumentException($"Excluding flavor '{flavor.Name}' needs a reason.", nameof(excluded));
            }
        }

        return All.Where(f => excluded.All(e => e.Flavor != f)).ToList();
    }

    /// <summary>Only <paramref name="flavors"/>, for a scenario about one provider's behavior.</summary>
    public static IReadOnlyList<ScenarioFlavor> Only(params ScenarioFlavor[] flavors)
    {
        if (flavors.Length == 0)
        {
            throw new ArgumentException("A scenario needs at least one flavor.", nameof(flavors));
        }

        return flavors;
    }

    public static ScenarioFlavor Get(string name)
    {
        return All.FirstOrDefault(f => string.Equals(f.Name, name, StringComparison.OrdinalIgnoreCase))
               ?? throw new ArgumentException($"Unknown flavor '{name}'. Known: {string.Join(", ", All.Select(f => f.Name))}.", nameof(name));
    }

    private static string Postgres(string database)
    {
        return $"Host={{{{PostgresHost}}}};Port={{{{PostgresPort}}}};Database={database};Username={{{{PostgresUsername}}}};Password={{{{PostgresPassword}}}}";
    }

    private static string MySql(string database)
    {
        return $"Server={{{{MySqlHost}}}};Port={{{{MySqlPort}}}};Database={database};User ID={{{{MySqlUsername}}}};Password={{{{MySqlPassword}}}};UseAffectedRows=True;AllowUserVariables=True";
    }

    private static string SqlServer(string database)
    {
        return $"Server={{{{SqlServerHost}}}};Database={database};User Id={{{{SqlServerUsername}}}};Password={{{{SqlServerPassword}}}};TrustServerCertificate=True;Max Pool Size=300;";
    }

    private static string RavenDb(string database)
    {
        return $"Urls={{{{RavenDbUrl}}}};Database={database}";
    }

    private static string Nats()
    {
        return "nats://{{NatsUsername}}:{{NatsPassword}}@{{NatsHost}}:{{NatsPort}}";
    }

    private static JsonObject Single(string repositoryType, string connectionString)
    {
        return new JsonObject
        {
            ["RepositoryType"] = repositoryType,
            ["ConnectionString"] = connectionString
        };
    }

    private static JsonObject Hybrid(string transportType, string transportConnection, string executionType, string executionConnection)
    {
        return new JsonObject
        {
            ["Transport"] = Single(transportType, transportConnection),
            ["Execution"] = Single(executionType, executionConnection)
        };
    }
}
