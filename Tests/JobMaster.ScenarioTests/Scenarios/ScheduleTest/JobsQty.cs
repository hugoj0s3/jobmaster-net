namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest;

/// <summary>
/// One batch of jobs to schedule: a quantity of a given handler type, against a given cluster,
/// optionally delayed. Built as a plan up front (before anything is scheduled), then used to drive
/// both the scheduling loop and every assertion afterward — per cluster and per handler.
/// </summary>
public sealed class JobsQty
{
    public int? AfterSecs { get; set; }
    public string ClusterId { get; set; } = null!;
    public int QtyJobs { get; set; }
    public string HandlerType { get; set; } = null!;
}
