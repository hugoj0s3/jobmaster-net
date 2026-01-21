using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Models.Agents;

[EditorBrowsable(EditorBrowsableState.Never)]
public enum AgentWorkerStatus
{
    Active,
    Stopping,
    Dead
}