using JobMaster.Contracts.Models;

namespace JobMaster.Contracts;

public interface IJobHandler
{
    Task HandleAsync(JobContext job);
}