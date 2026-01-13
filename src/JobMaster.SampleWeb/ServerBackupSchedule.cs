using JobMaster.Contracts;
using JobMaster.Contracts.Models;
using JobMaster.Contracts.RecurrenceExpressions;

namespace JobMaster.SampleWeb;

public class ServerBackupHandler : IJobHandler
{
    public Task HandleAsync(JobContext job)
    {
        return Task.CompletedTask;
    }
}
