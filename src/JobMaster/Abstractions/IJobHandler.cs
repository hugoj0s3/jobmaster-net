using JobMaster.Abstractions.Models;

namespace JobMaster.Abstractions;

public interface IJobHandler
{
    Task HandleAsync(JobContext job);
}