namespace JobMaster.Abstractions.Models;

public enum AgentWorkerMode
{
    Full = 1,
    Execution = 2,
    Drain = 3,
    Coordinator = 4,
}