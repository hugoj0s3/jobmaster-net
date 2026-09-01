using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services.Master;

internal class MasterJobIntakeService : JobMasterClusterAwareComponent, IMasterJobIntakeService
{
    private readonly IMasterJobsRepository masterJobsRepository;
    private readonly IMasterLogsRepository masterLogsRepository;
    private readonly IMasterClusterConfigurationService masterClusterConfigurationService;

    public MasterJobIntakeService(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IMasterJobsRepository masterJobsRepository,
        IMasterLogsRepository masterLogsRepository,
        IMasterClusterConfigurationService masterClusterConfigurationService) : base(clusterConnectionConfig)
    {
        this.masterJobsRepository = masterJobsRepository;
        this.masterLogsRepository = masterLogsRepository;
        this.masterClusterConfigurationService = masterClusterConfigurationService;
    }

    public async Task BulkInsertIfNotExistsAsync(IList<JobRawModel> jobs, IList<JobExecution> jobExecutions, IList<LogItem> jobExecutionLogs)
    {
        var clusterMode = masterClusterConfigurationService.Get()?.ClusterMode;

        switch (clusterMode)
        {
            case ClusterMode.Archived:
            {
                var nonFinal = jobs.Where(j => !j.Status.IsFinalStatus()).ToList();
                if (nonFinal.Count > 0)
                {
                    throw new InvalidOperationException(
                        $"Cannot archive {nonFinal.Count} job(s) not in a final status " +
                        $"(e.g. job '{nonFinal[0].Id}' has status '{nonFinal[0].Status}'). " +
                        "Only finalized jobs can be archived.");
                }
                break;
            }
            case ClusterMode.Active:
            {
                var notOnMaster = jobs.Where(j => j.Status != JobMasterJobStatus.OnMaster).ToList();
                if (notOnMaster.Count > 0)
                {
                    throw new InvalidOperationException(
                        $"Cannot migrate {notOnMaster.Count} job(s) not in OnMaster status " +
                        $"(e.g. job '{notOnMaster[0].Id}' has status '{notOnMaster[0].Status}'). " +
                        "Only held (OnMaster) jobs can be migrated.");
                }
                break;
            }
            default:
                throw new InvalidOperationException(
                    $"Cluster '{this.ClusterConnConfig.ClusterId}' cannot receive jobs from another cluster " +
                    $"while in ClusterMode '{clusterMode}'. Only Archived and Active clusters can.");
        }

        foreach (var job in jobs)
        {
            job.ReassignToCluster(this.ClusterConnConfig.ClusterId);
        }

        foreach (var execution in jobExecutions)
        {
            execution.ReassignToCluster(this.ClusterConnConfig.ClusterId);
        }

        var insertedJobIds = await masterJobsRepository.BulkInsertIfNotExistsAsync(jobs, jobExecutions);

        // Logs aren't stored by IMasterJobsRepository, so they need their own "only newly-inserted jobs"
        // filter here, using exactly the set the repository just reported it actually inserted -- avoids a
        // second, potentially racy existence check against jobs the repository already resolved.
        // "N" format (no dashes) -- matches how JobMasterLoggerExtensions stores ReferenceId for every
        // JobExecution-category log (referenceId.ToString("N")), which is what jobExecutionLogs' entries
        // were queried by upstream.
        var insertedJobIdStrings = new HashSet<string>(insertedJobIds.Select(id => id.ToString("N")));
        var logsForNewlyInsertedJobs = jobExecutionLogs.Where(l => l.ReferenceId != null && insertedJobIdStrings.Contains(l.ReferenceId)).ToList();
        foreach (var log in logsForNewlyInsertedJobs)
        {
            log.ClusterId = this.ClusterConnConfig.ClusterId;
        }

        if (logsForNewlyInsertedJobs.Count > 0)
        {
            await masterLogsRepository.BulkInsertAsync(logsForNewlyInsertedJobs);
        }
    }
}
