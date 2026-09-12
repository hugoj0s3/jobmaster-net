namespace JobMaster.Sdk.Abstractions.Models.Jobs;

internal class JobProbeResult
{
    public long Count { get; set; }
    public DateTime? MinNextPlanExecutionAt { get; set; }
}
