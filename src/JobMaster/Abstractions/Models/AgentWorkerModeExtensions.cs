namespace JobMaster.Abstractions.Models;

internal static class AgentWorkerModeExtensions
{
    /// <summary>Full also runs the coordinator/execution/drain-job runners alongside the mode it's compared to, so it always satisfies an Is check.</summary>
    public static bool IsFullOr(this AgentWorkerMode self, AgentWorkerMode mode) => self == mode || self == AgentWorkerMode.Full;
}
