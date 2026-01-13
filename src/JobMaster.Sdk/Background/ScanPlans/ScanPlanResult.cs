namespace JobMaster.Sdk.Background.ScanPlans;

internal sealed class ScanPlanResult
{
    public ScanPlanResult()
    {
        CalculatedAt = DateTime.UtcNow;
    }
    public int BatchSize { get; set; }
    public TimeSpan Interval { get; set; }   // [10s..10m]
    public int LockerMin { get; set; }       // inclusive
    public int LockerMax { get; set; }       // inclusive
    
    public DateTime CalculatedAt { get; set; }

    // If older than 2 minutes, calculate again
    public bool ShouldCalculateAgain()
    {
        return CalculatedAt < DateTime.UtcNow.AddMinutes(-2);
    }
}