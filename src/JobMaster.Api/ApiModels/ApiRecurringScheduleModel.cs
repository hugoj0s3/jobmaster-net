using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Api.ApiModels;

/// <summary>Represents a recurring schedule as returned by the API.</summary>
public class ApiRecurringScheduleModel : ApiClusterBaseModel
{
    /// <summary>Unique identifier of the schedule (base64url-encoded GUID).</summary>
    public string Id { get; set; } = string.Empty;
    /// <summary>Raw recurrence expression string (e.g. a cron expression).</summary>
    public string Expression { get; set; } = string.Empty;
    /// <summary>Compiler type identifier that interprets the expression.</summary>
    public string ExpressionTypeId { get; set; } = string.Empty;
    /// <summary>Handler definition ID fired on each occurrence.</summary>
    public string JobDefinitionId { get; set; } = string.Empty;
    /// <summary>Static definition ID, if this schedule originates from a static profile.</summary>
    public string? StaticDefinitionId { get; set; }
    /// <summary>Profile ID, if this schedule belongs to a static recurring schedule profile.</summary>
    public string? ProfileId { get; set; }
    /// <summary>Current lifecycle status of the schedule.</summary>
    public RecurringScheduleStatus Status { get; set; }
    /// <summary>Whether this is a static (startup-defined) or dynamic (runtime-created) schedule.</summary>
    public RecurringScheduleType RecurringScheduleType { get; set; }
    /// <summary>UTC timestamp when the schedule was terminated, if applicable.</summary>
    public DateTime? TerminatedAt { get; set; }
    /// <summary>Message payload passed to the handler on each occurrence.</summary>
    public IDictionary<string, object?> MsgData { get; set; } = new Dictionary<string, object?>();
    /// <summary>Key-value metadata passed to the handler on each occurrence.</summary>
    public IDictionary<string, object?> Metadata { get; set; } = new Dictionary<string, object?>();
    /// <summary>Execution priority per occurrence, or <c>null</c> to use the handler default.</summary>
    public JobMasterPriority? Priority { get; set; }
    /// <summary>Max retries per occurrence, or <c>null</c> to use the handler default.</summary>
    public int? MaxNumberOfRetries { get; set; }
    /// <summary>Max execution time per occurrence, or <c>null</c> to use the handler default.</summary>
    public TimeSpan? Timeout { get; set; }
    /// <summary>UTC timestamp when the schedule was created.</summary>
    public DateTime CreatedAt { get; set; }
    /// <summary>UTC date before which the schedule will not fire.</summary>
    public DateTime? StartAfter { get; set; }
    /// <summary>UTC date after which the schedule will not fire.</summary>
    public DateTime? EndBefore { get; set; }
    /// <summary>UTC timestamp up to which jobs have been planned by the scheduler.</summary>
    public DateTime? LastPlanCoverageUntil { get; set; }
    /// <summary>UTC timestamp of the last plan execution run.</summary>
    public DateTime? LastExecutedPlan { get; set; }
    /// <summary>Whether the last plan execution encountered a failure.</summary>
    public bool? HasFailedOnLastPlanExecution { get; set; }
    /// <summary>Whether a job cancellation is currently pending for this schedule.</summary>
    public bool? IsJobCancellationPending { get; set; }
    /// <summary>UTC timestamp when the static definition was last confirmed/ensured, if applicable.</summary>
    public DateTime? StaticDefinitionLastEnsured { get; set; }
    /// <summary>Worker lane jobs are routed to, or <c>null</c> for the default lane.</summary>
    public string? WorkerLane { get; set; }
    /// <summary>Whether this is a static schedule that is currently idle (not <see cref="RecurringScheduleStatus.Active"/>).</summary>
    public bool IsStaticIdle { get; set; }

    internal static ApiRecurringScheduleModel FromDomain(RecurringSchedule schedule)
    {
        var isStaticIdle = false;
        if (schedule.RecurringScheduleType == RecurringScheduleType.Static)
        {
            if (schedule.Status != RecurringScheduleStatus.Active)
            {
                isStaticIdle = true;
            }
            else
            {
                var afterAt = DateTime.UtcNow.AddMinutes(-5);
                isStaticIdle = schedule.StaticDefinitionLastEnsured < afterAt;
            }
        }

        return new ApiRecurringScheduleModel
        {
            ClusterId = schedule.ClusterId,
            Id = schedule.Id.ToBase64(),
            Expression = schedule.RecurExpression.Expression,
            ExpressionTypeId = schedule.RecurExpression.ExpressionTypeId,
            JobDefinitionId = schedule.JobDefinitionId,
            StaticDefinitionId = schedule.StaticDefinitionId,
            ProfileId = schedule.ProfileId,
            Status = schedule.Status,
            RecurringScheduleType = schedule.RecurringScheduleType,
            TerminatedAt = schedule.TerminatedAt,
            MsgData = schedule.MsgData.ToDictionary(),
            Metadata = schedule.Metadata?.ToDictionary() ?? new Dictionary<string, object?>(),
            Priority = schedule.Priority,
            MaxNumberOfRetries = schedule.MaxNumberOfRetries,
            Timeout = schedule.Timeout,
            CreatedAt = schedule.CreatedAt,
            StartAfter = schedule.StartAfter,
            EndBefore = schedule.EndBefore,
            LastPlanCoverageUntil = schedule.LastPlanCoverageUntil,
            LastExecutedPlan = schedule.LastExecutedPlan,
            HasFailedOnLastPlanExecution = schedule.HasFailedOnLastPlanExecution,
            IsJobCancellationPending = schedule.IsJobCancellationPending,
            StaticDefinitionLastEnsured = schedule.StaticDefinitionLastEnsured,
            WorkerLane = schedule.WorkerLane,
            IsStaticIdle = isStaticIdle,
        };
    }

    internal static ApiRecurringScheduleModel FromDomain(RecurringScheduleRawModel scheduleRawModel)
    {
        return FromDomain(RecurringScheduleConvertUtil.ToRecurringSchedule(scheduleRawModel));
    }
}