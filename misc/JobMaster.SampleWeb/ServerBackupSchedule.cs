using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;

namespace JobMaster.SampleWeb;

public class ServerBackupMasterHandler : IJobMasterHandler
{
    public Task HandleAsync(JobContext job)
    {
        return Task.CompletedTask;
    }
}
