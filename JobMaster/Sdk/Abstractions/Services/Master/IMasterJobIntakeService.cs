using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;

namespace JobMaster.Sdk.Abstractions.Services.Master;

/// <summary>
/// Runs on the receiving side of a cross-cluster job transfer — either an
/// <see cref="Abstractions.Models.ClusterMode.Archived"/> cluster (finalized jobs being purged
/// elsewhere) or an <see cref="Abstractions.Models.ClusterMode.Active"/> cluster (jobs being migrated
/// off a <see cref="Abstractions.Models.ClusterMode.Migrating"/> cluster). Resolved cross-cluster via
/// <see cref="Ioc.IJobMasterClusterAwareComponentFactory.GetComponent{TComponent}"/> — the caller never
/// touches this cluster's <see cref="JobMaster.Sdk.Abstractions.Repositories.Master.IMasterJobsRepository"/> directly.
/// </summary>
internal interface IMasterJobIntakeService : IJobMasterClusterAwareService
{
    /// <summary>
    /// Inserts each job that doesn't already exist in this cluster, reassigning it here first. The
    /// required incoming status depends on this cluster's own mode: a final status on an Archived
    /// cluster, <c>OnMaster</c> on an Active cluster (receiving a migrated job) — throws otherwise.
    /// Existing rows are left untouched. <paramref name="jobExecutions"/> and
    /// <paramref name="jobExecutionLogs"/> travel alongside their parent job — only copied for jobs that
    /// were actually newly inserted here, silently dropped for jobs that already existed.
    /// </summary>
    Task BulkInsertIfNotExistsAsync(IList<JobRawModel> jobs, IList<JobExecution> jobExecutions, IList<LogItem> jobExecutionLogs);
}
