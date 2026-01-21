using JobMaster.Abstractions.Models;

namespace JobMaster.IntegrationTests;

public class QtyJobsTests
{
    public int QtyJobs { get; set; }
    public string ClusterId { get; set; } = string.Empty;
    public JobMasterPriority Priority { get; set; }
    public string? WorkerLane { get; set; }
}