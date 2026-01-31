using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Jobs;

namespace JobMaster.Api.ApiModels;

public class ApiRecurringScheduleModel : ApiClusterBaseModel
{
    public string Id { get; set; } = string.Empty;
    
    public string Expression { get; set; } = string.Empty;
    
    public string ExpressionTypeId { get; set; } = string.Empty;
    
    public string JobDefinitionId { get; set; } = string.Empty;
    
    public string? StaticDefinitionId { get; set; }
    
    public string? ProfileId { get; set; }
    
    public RecurringScheduleStatus Status { get; set; }
    
    public RecurringScheduleType RecurringScheduleType { get; set; }
    
    public DateTime? TerminatedAt { get; set; }
    
    public IDictionary<string, object?> MsgData { get; set; } = new Dictionary<string, object?>();
    
    public IDictionary<string, object?> Metadata { get; set; } = new Dictionary<string, object?>();
    
    public JobMasterPriority? Priority { get; set; }
    
    public int? MaxNumberOfRetries { get; set; }
    
    public TimeSpan? Timeout { get; set; }
    
    public DateTime CreatedAt { get; set; }
    
    public DateTime? StartAfter { get; set; }
    
    public DateTime? EndBefore { get; set; }
    
    public DateTime? LastPlanCoverageUntil { get; set; }
    
    public DateTime? LastExecutedPlan { get; set; }
    
    public bool? HasFailedOnLastPlanExecution { get; set; }
    
    public bool? IsJobCancellationPending { get; set; }
    
    public DateTime? StaticDefinitionLastEnsured { get; set; }
    
    public string? WorkerLane { get; set; }
    
    public bool IsStaticIdle { get; set; }

    internal static ApiRecurringScheduleModel FromDomain(RecurringSchedule schedule)
    {
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
            IsStaticIdle = schedule.RecurringScheduleType == RecurringScheduleType.Static && schedule.Status != RecurringScheduleStatus.Active,
        };
    }
}