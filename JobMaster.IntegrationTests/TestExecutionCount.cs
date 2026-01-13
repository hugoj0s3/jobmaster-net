namespace JobMaster.IntegrationTests;

public class TestExecutionCount
{
    public string TestExecutionId { get; set; } = string.Empty;
    public int TotalExecuted { get; set; }
    public int TotalDuplicates { get; set; }
    public Dictionary<Guid, int> JobExecutionCounts { get; set; } = new Dictionary<Guid, int>();
    public Dictionary<Guid, DateTime> FirstExecutionTime { get; set; } = new Dictionary<Guid, DateTime>();
    public Dictionary<Guid, DateTime> LastExecutionTime { get; set; } = new Dictionary<Guid, DateTime>();
}
