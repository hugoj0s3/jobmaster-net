namespace JobMaster.Contracts.Models;

public enum AgentWorkerMode
{
    Standalone = 1,
    Execution = 2,
    Drain = 3,
    Coordinator = 4,
}