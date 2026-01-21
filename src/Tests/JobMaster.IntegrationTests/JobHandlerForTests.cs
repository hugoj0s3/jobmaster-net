using System.Collections.Concurrent;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;

namespace JobMaster.IntegrationTests;

[JobMasterDefinitionId("JobHandlerForTests")]
public class JobHandlerForTests : IJobHandler
{
    public static ConcurrentDictionary<string, TestExecutionCount> ExecutionsByTestId = new ConcurrentDictionary<string, TestExecutionCount>();
    
    public static TestExecutionCount? GetExecutionCount(string testExecutionId)
    {
        return ExecutionsByTestId.TryGetValue(testExecutionId, out var count) ? count : null;
    }
    
    public Task HandleAsync(JobContext job)
    {
        var testExecutionId = job.Metadata.GetStringValue("TestExecutionId");
        
        if (string.IsNullOrEmpty(testExecutionId))
        {
            // Job not part of a test execution, ignore
            return Task.CompletedTask;
        }
        
        var executionCount = ExecutionsByTestId.GetOrAdd(testExecutionId, _ => new TestExecutionCount
        {
            TestExecutionId = testExecutionId
        });
        
        var now = DateTime.UtcNow;
        
        lock (executionCount)
        {
            executionCount.TotalExecuted++;
            
            var currentCount = executionCount.JobExecutionCounts.GetValueOrDefault(job.Id, 0);
            executionCount.JobExecutionCounts[job.Id] = currentCount + 1;
            
            if (currentCount > 0)
            {
                executionCount.TotalDuplicates++;
            }
            
            if (!executionCount.FirstExecutionTime.ContainsKey(job.Id))
            {
                executionCount.FirstExecutionTime[job.Id] = now;
            }
            
            executionCount.LastExecutionTime[job.Id] = now;
        }
        
        return Task.CompletedTask;
    }
}