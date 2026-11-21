using System.Text.Json.Nodes;
using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioMatrixTests.Flavors;

/// <summary>
/// One provider combination a templated scenario can run on: the master provider, and what each agent
/// connection slot holds (a single provider, or a hybrid transport + execution pair).
/// </summary>
public sealed class ScenarioFlavor
{
    private readonly Func<string, string> masterConnectionString;
    private readonly Func<ScenarioFlavor, int, JsonObject> agent;

    public ScenarioFlavor(
        string name,
        string code,
        string masterRepoType,
        Func<string, string> masterConnectionString,
        Func<ScenarioFlavor, int, JsonObject> agent)
    {
        Name = name;
        Code = code;
        MasterRepoType = masterRepoType;
        this.masterConnectionString = masterConnectionString;
        this.agent = agent;
    }

    /// <summary>Readable name, shown in the test name (e.g. <c>PostgresNats</c>).</summary>
    public string Name { get; }

    /// <summary>Short code for ids and names (cluster ids are limited to 25 characters).</summary>
    public string Code { get; }

    public string MasterRepoType { get; }

    /// <summary>
    /// A database name unique to this flavor and the matrix project, so flavors (and the regular scenario
    /// suite) sharing one engine never write to the same database.
    /// </summary>
    public string DatabaseName(string logicalName)
    {
        return "Mx" + char.ToUpperInvariant(Code[0]) + Code.Substring(1) + logicalName;
    }

    public string MasterConnectionString(string logicalDatabaseName)
    {
        return masterConnectionString(DatabaseName(logicalDatabaseName));
    }

    /// <summary>The agent connection's fields for <paramref name="slot"/> (1-based), without its name.</summary>
    public JsonObject Agent(int slot)
    {
        return agent(this, slot);
    }

    /// <summary>
    /// Writes <paramref name="mode"/> (a <c>ClusterMode</c> value) as <paramref name="clusterId"/>'s stored mode in the
    /// master database <paramref name="logicalDatabaseName"/>, directly, bypassing JobMaster. Returns the rows changed.
    /// </summary>
    public Task<int> SetStoredClusterModeAsync(ScenarioGlobalEnvironment global, string logicalDatabaseName, string clusterId, int mode)
    {
        return MasterStoreRaw.SetStoredClusterModeAsync(global, MasterRepoType, DatabaseName(logicalDatabaseName), clusterId, mode);
    }

    public override string ToString()
    {
        return Name;
    }
}
