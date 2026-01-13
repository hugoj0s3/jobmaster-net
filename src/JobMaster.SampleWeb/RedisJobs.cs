using StackExchange.Redis;

namespace JobMaster.SampleWeb;

public static class RedisJobs
{
    public static ConnectionMultiplexer RedisConnection = ConnectionMultiplexer.Connect("localhost:11001,DefaultDatabase=0");
    public static IDatabase RedisDatabase = RedisConnection.GetDatabase();

    public static IList<Guid> GetJobExecutedIds()
    { 
        var entries = RedisDatabase.HashGetAll("JobExecutedIds");
        return entries
            .Select(entry => Guid.TryParse(entry.Value, out var guid) ? guid : Guid.Empty)
            .Where(x => x != Guid.Empty)
            .ToList();
    }
    
    public static IList<Guid> GetScheduledJobIds()
    {
        var entries = RedisDatabase.HashGetAll("ScheduledJobIds");
        return entries.Select(entry => Guid.TryParse(entry.Value, out var guid) ? guid : Guid.Empty)
            .Where(x => x != Guid.Empty)
            .ToList();
    }
    
    public static void AddJobExecutedId(Guid id)
    {
        RedisDatabase.HashSet("JobExecutedIds", id.ToString(), id.ToString());
    }
    
    public static void AddScheduledJobId(Guid id)
    {
        RedisDatabase.HashSet("ScheduledJobIds", id.ToString(), id.ToString());
    }
    
    public static void RemoveScheduledJobId(Guid id)
    {
        RedisDatabase.HashDelete("ScheduledJobIds", id.ToString());
    }

    public static void ClearScheduledJobIds()
    {
        var scheduledJobIds = GetScheduledJobIds();
        foreach (var scheduledJobId in scheduledJobIds)
        {
            RemoveScheduledJobId(scheduledJobId);
        }
    }
    
    public static void ClearJobExecutedIds()
    {
        var jobExecutedIds = GetJobExecutedIds();
        foreach (var jobExecutedId in jobExecutedIds)
        {
            RedisDatabase.HashDelete("JobExecutedIds", jobExecutedId.ToString());
        }
    }
}