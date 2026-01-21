using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;

namespace JobMaster.SampleWeb;

public class ServerBackupHandler : IJobHandler
{
    public Task HandleAsync(JobContext job)
    {
        return Task.CompletedTask;
    }
}
