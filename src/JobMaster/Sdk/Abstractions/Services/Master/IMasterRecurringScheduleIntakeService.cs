using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Services.Master;

/// <summary>
/// Runs on the receiving side of a cross-cluster recurring-schedule transfer — either an
/// <see cref="Abstractions.Models.ClusterMode.Archived"/> cluster (finalized schedules being purged
/// elsewhere) or an <see cref="Abstractions.Models.ClusterMode.Active"/> cluster (schedules being
/// migrated off a <see cref="Abstractions.Models.ClusterMode.Migrating"/> cluster). Resolved
/// cross-cluster via <see cref="Ioc.IJobMasterClusterAwareComponentFactory.GetComponent{TComponent}"/> —
/// the caller never touches this cluster's
/// <see cref="JobMaster.Sdk.Abstractions.Repositories.Master.IMasterRecurringSchedulesRepository"/> directly.
/// </summary>
internal interface IMasterRecurringScheduleIntakeService : IJobMasterClusterAwareService
{
    /// <summary>
    /// Inserts each recurring schedule that doesn't already exist in this cluster, reassigning it here
    /// first. The required incoming status depends on this cluster's own mode: a final status on an
    /// Archived cluster, <c>Active</c> on an Active cluster (receiving a migrated schedule) — throws
    /// otherwise. Existing rows are left untouched.
    /// </summary>
    Task BulkInsertIfNotExistsAsync(IList<RecurringScheduleRawModel> schedules);
}
