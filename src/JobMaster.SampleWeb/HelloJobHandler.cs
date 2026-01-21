using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;

namespace JobMaster.SampleWeb;

[JobMasterMetadata("Attribute1", 10)]
[JobMasterDefinitionId("HelloJob")]
[JobMasterTimeout(10)]
[JobMasterMaxNumberOfRetries(3)]
[JobMasterPriority(JobMasterPriority.Medium)]
public class HelloJobHandler : IJobHandler
{
    public static int Counter = 0;
    
    public HelloJobHandler()
    {
    }

    public Task HandleAsync(JobContext job)
    {
        var name = job.MsgData.TryGetStringValue("Name") ?? string.Empty;
        if (string.IsNullOrWhiteSpace(name))
        {
            name = "World";
        }
        
        SayHello(name, job.ScheduledAt);
        var metadata = job.Metadata.TryGetStringValue("MyMetadata");
        Console.WriteLine($"MyMetadata: {metadata}");
        
        var attribute1 = job.Metadata.TryGetIntValue("Attribute1");
        Console.WriteLine($"Attribute1: {attribute1}");
        
        var expression = job.RecurringSchedule?.Metadata.TryGetStringValue("expression");
        Console.WriteLine($"Expression: {expression}");
        
        return Task.CompletedTask;
    }

    public static void SayHello(string name, DateTime scheduledAt)
    {
        var delay = DateTime.UtcNow - scheduledAt;

        Console.WriteLine($"Hello {name} at {DateTime.UtcNow}, scheduled at {scheduledAt}. Count: {++Counter}, Delay: {delay}");
        
        Console.ResetColor();
    }
}

[JobMasterMetadata("Attribute1", 10)]
[JobMasterWorkerLane("Lane1")]
public class HelloJobHandler2 : IJobHandler
{
    public static int Counter = 0;
    
    public HelloJobHandler2()
    {
    }

    public Task HandleAsync(JobContext job)
    {
        var name = job.MsgData.TryGetStringValue("Name") ?? string.Empty;
        if (string.IsNullOrWhiteSpace(name))
        {
            name = Faker.Name.FullName();
        }
        
        SayHello(name, job.ScheduledAt);
        var metadata = job.Metadata.TryGetStringValue("MyMetadata");
        Console.WriteLine($"MyMetadata: {metadata}");
        
        var attribute1 = job.Metadata.TryGetIntValue("Attribute1");
        Console.WriteLine($"Attribute1: {attribute1}");
        
        var expression = job.RecurringSchedule?.Metadata.TryGetStringValue("expression");
        Console.WriteLine($"Expression: {expression}");
        
        Console.WriteLine("Lane: " + job.WorkerLane);
        
        return Task.CompletedTask;
    }

    public static void SayHello(string name, DateTime scheduledAt)
    {
        var delay = DateTime.UtcNow - scheduledAt;
        
        Console.WriteLine($"Hello {name} at {DateTime.UtcNow}, scheduled at {scheduledAt}. Count: {++Counter}, Delay: {delay}");
        
        Console.ResetColor();
    }
}