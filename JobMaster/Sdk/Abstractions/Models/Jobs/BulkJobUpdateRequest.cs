using System.Linq.Expressions;
using JobMaster.Abstractions.Models;

namespace JobMaster.Sdk.Abstractions.Models.Jobs;

/// <summary>
/// Describes the fields to update in a bulk job update operation.
/// Only the explicitly provided fields are included in the SQL SET clause.
/// Version is always regenerated on every bulk update.
/// </summary>
internal sealed class BulkJobUpdateRequest
{
    private BulkJobUpdateRequest() { }

    public IList<Guid> JobIds { get; private set; } = new List<Guid>();
    public IReadOnlyList<BulkJobUpdateProperty> Properties { get; private set; } = Array.Empty<BulkJobUpdateProperty>();
    public IList<JobMasterJobStatus>? ExcludeStatuses { get; private set; }

    public static BulkJobUpdateRequest For(
        IList<Guid> jobIds,
        params BulkJobUpdateProperty[] fields)
        => new() { JobIds = jobIds, Properties = fields };

    public static BulkJobUpdateRequest For(
        IList<Guid> jobIds,
        IList<JobMasterJobStatus> excludeStatuses,
        params BulkJobUpdateProperty[] fields)
        => new() { JobIds = jobIds, Properties = fields, ExcludeStatuses = excludeStatuses };

    public static BulkJobUpdateRequest Cancel(IList<Guid> jobIds)
    {
        var finalStatuses = JobMasterJobStatusUtil.GetFinalStatuses().ToList();
        return For(
            jobIds,
            excludeStatuses: finalStatuses,
            BulkJobUpdateProperty.For(j => j.Status, JobMasterJobStatus.Cancelled),
            BulkJobUpdateProperty.For(j => j.AgentConnectionId, null),
            BulkJobUpdateProperty.For(j => j.AgentWorkerId, null),
            BulkJobUpdateProperty.For(j => j.BucketId, null));
    }

    public static BulkJobUpdateRequest HeldOnMaster(IList<Guid> jobIds)
        => For(
            jobIds,
            excludeStatuses: [JobMasterJobStatus.OnMaster],
            BulkJobUpdateProperty.For(j => j.Status, JobMasterJobStatus.OnMaster),
            BulkJobUpdateProperty.For(j => j.AgentConnectionId, null),
            BulkJobUpdateProperty.For(j => j.AgentWorkerId, null),
            BulkJobUpdateProperty.For(j => j.BucketId, null),
            BulkJobUpdateProperty.For(j => j.ProcessDeadline, null),
            BulkJobUpdateProperty.For(j => j.PartitionLockId, null),
            BulkJobUpdateProperty.For(j => j.PartitionLockExpiresAt, null));
}

/// <summary>
/// Pairs a field selector expression with the value to write for that field.
/// Used to build a partial bulk update where each field carries its own value,
/// applied uniformly to all rows in the batch.
/// </summary>
internal sealed class BulkJobUpdateProperty
{
    private BulkJobUpdateProperty(Expression<Func<JobRawModel, object?>> expression, object? value)
    {
        Expression = expression;
        Value = value;
    }

    /// <summary>Expression identifying the column to update (used for column-name derivation).</summary>
    public Expression<Func<JobRawModel, object?>> Expression { get; }

    /// <summary>The value to write into that column for every row in this batch.</summary>
    public object? Value { get; }

    /// <summary>
    /// Creates a field+value pair. The typed expression is wrapped in a Convert node so that
    /// <c>sql.ColumnNameFor</c> can extract the member name regardless of the property's declared type.
    /// </summary>
    public static BulkJobUpdateProperty For<T>(Expression<Func<JobRawModel, T>> expression, object? value)
    {
        var body = System.Linq.Expressions.Expression.Convert(expression.Body, typeof(object));
        var converted = System.Linq.Expressions.Expression.Lambda<Func<JobRawModel, object?>>(body, expression.Parameters);
        return new BulkJobUpdateProperty(converted, value);
    }
}
