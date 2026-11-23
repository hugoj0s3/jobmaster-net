namespace JobMaster.ScenarioMatrixTests.Lib;

/// <summary>How a scenario's cluster is set up.</summary>
public enum ScenarioTopology
{
    /// <summary>
    /// <c>UseStandaloneCluster()</c>: one connection, the master database, does everything; no agent connection.
    /// Runs only on flavors whose agent side is the master provider (see <c>ScenarioFlavors.StandaloneHasNoAgent</c>).
    /// </summary>
    Standalone,

    /// <summary>A regular cluster: master database plus the flavor's agent connections.</summary>
    Distributed
}
