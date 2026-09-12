using System.Reflection;
using System.Text;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Utils;

namespace JobMaster.Abstractions.StaticRecurringSchedules;

/// <summary>
/// Mutable collector for static recurring schedules declared by profiles.
/// - Enforces uniqueness per (clusterId, id)
/// - Provides helpers to add entries using compiled or text expressions
/// - Exposes a read-only snapshot for bootstrap consumption
/// </summary>
public sealed class RecurringScheduleDefinitionCollection
{
    private readonly List<StaticRecurringScheduleDefinition> items = new();
    private readonly ISet<(string ClusterId, string Id)> unique = new HashSet<(string, string)>();
    private readonly Dictionary<(string ClusterId, string HandlerKey), int> seqByHandler = new();

    private readonly StaticRecurringSchedulesProfileInfo profile;
    private readonly string defaultClusterId;

    /// <summary>Initializes the collection scoped to the given profile and default cluster.</summary>
    public RecurringScheduleDefinitionCollection(StaticRecurringSchedulesProfileInfo profile, string defaultClusterId)
    {
        if (string.IsNullOrWhiteSpace(defaultClusterId))
            throw new ArgumentException("Default clusterId is required", nameof(defaultClusterId));
        this.profile = profile;
        this.defaultClusterId = defaultClusterId;
    }

    /// <summary>Returns a read-only snapshot of all registered definitions.</summary>
    public IReadOnlyList<StaticRecurringScheduleDefinition> ToReadOnly() => items;

    /// <summary>
    /// Adds a recurring schedule for <typeparamref name="Th"/> using a text-based recurrence expression.
    /// </summary>
    /// <param name="expressionType">The recurrence compiler type ID (e.g. <c>"Cron"</c>).</param>
    /// <param name="expression">The raw recurrence expression string.</param>
    /// <param name="defId">Optional unique definition ID within this profile. Auto-generated when null.</param>
    /// <param name="priority">Execution priority per occurrence. Defaults to the handler attribute or cluster default.</param>
    /// <param name="timeout">Maximum execution time per occurrence. Defaults to the handler attribute or cluster default.</param>
    /// <param name="maxNumberOfRetries">Max retries per occurrence. Defaults to the handler attribute or cluster default.</param>
    /// <param name="startAfter">UTC date before which no jobs fire.</param>
    /// <param name="endBefore">UTC date after which no jobs fire.</param>
    /// <param name="metadata">Optional key-value metadata attached to every job occurrence.</param>
    public RecurringScheduleDefinitionCollection Add<Th>(
        string expressionType,
        string expression,
        string? defId = null,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        IWritableMetadata? metadata = null)
        where Th : class, IJobMasterHandler
        => Add(typeof(Th), expressionType, expression, defId, priority, timeout, maxNumberOfRetries, startAfter, endBefore, metadata);

    /// <summary>
    /// Adds a recurring schedule for <paramref name="handlerType"/> using a text-based recurrence expression.
    /// Non-generic counterpart of <see cref="Add{Th}(string, string, string?, JobMasterPriority?, TimeSpan?, int?, DateTime?, DateTime?, IWritableMetadata?)"/>
    /// for callers that only have a <see cref="Type"/> at hand (e.g. reflection-driven registration).
    /// </summary>
    /// <param name="handlerType">The job handler type. Must implement <see cref="IJobMasterHandler"/>.</param>
    /// <param name="expressionType">The recurrence compiler type ID (e.g. <c>"Cron"</c>).</param>
    /// <param name="expression">The raw recurrence expression string.</param>
    /// <param name="defId">Optional unique definition ID within this profile. Auto-generated when null.</param>
    /// <param name="priority">Execution priority per occurrence. Defaults to the handler attribute or cluster default.</param>
    /// <param name="timeout">Maximum execution time per occurrence. Defaults to the handler attribute or cluster default.</param>
    /// <param name="maxNumberOfRetries">Max retries per occurrence. Defaults to the handler attribute or cluster default.</param>
    /// <param name="startAfter">UTC date before which no jobs fire.</param>
    /// <param name="endBefore">UTC date after which no jobs fire.</param>
    /// <param name="metadata">Optional key-value metadata attached to every job occurrence.</param>
    public RecurringScheduleDefinitionCollection Add(
        Type handlerType,
        string expressionType,
        string expression,
        string? defId = null,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        IWritableMetadata? metadata = null)
    {
        var compiled = RecurrenceExprCompiler.Compile(expressionType, expression);
        return Add(handlerType, compiled, defId, priority, timeout, maxNumberOfRetries, startAfter, endBefore, metadata);
    }

    /// <summary>
    /// Adds a recurring schedule for <typeparamref name="Th"/> using a pre-compiled recurrence expression.
    /// </summary>
    /// <param name="compiledExpr">The already-compiled recurrence expression.</param>
    /// <param name="defId">Optional unique definition ID within this profile. Auto-generated when null.</param>
    /// <param name="priority">Execution priority per occurrence.</param>
    /// <param name="timeout">Maximum execution time per occurrence.</param>
    /// <param name="maxNumberOfRetries">Max retries per occurrence.</param>
    /// <param name="startAfter">UTC date before which no jobs fire.</param>
    /// <param name="endBefore">UTC date after which no jobs fire.</param>
    /// <param name="metadata">Optional key-value metadata attached to every job occurrence.</param>
    public RecurringScheduleDefinitionCollection Add<Th>(
        IRecurrenceCompiledExpr compiledExpr,
        string? defId = null,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        IWritableMetadata? metadata = null)
        where Th : class, IJobMasterHandler
        => Add(typeof(Th), compiledExpr, defId, priority, timeout, maxNumberOfRetries, startAfter, endBefore, metadata);

    /// <summary>
    /// Adds a recurring schedule for <paramref name="handlerType"/> using a pre-compiled recurrence expression.
    /// Non-generic counterpart of <see cref="Add{Th}(IRecurrenceCompiledExpr, string?, JobMasterPriority?, TimeSpan?, int?, DateTime?, DateTime?, IWritableMetadata?)"/>
    /// for callers that only have a <see cref="Type"/> at hand (e.g. reflection-driven registration).
    /// </summary>
    /// <param name="handlerType">The job handler type. Must implement <see cref="IJobMasterHandler"/>.</param>
    /// <param name="compiledExpr">The already-compiled recurrence expression.</param>
    /// <param name="defId">Optional unique definition ID within this profile. Auto-generated when null.</param>
    /// <param name="priority">Execution priority per occurrence.</param>
    /// <param name="timeout">Maximum execution time per occurrence.</param>
    /// <param name="maxNumberOfRetries">Max retries per occurrence.</param>
    /// <param name="startAfter">UTC date before which no jobs fire.</param>
    /// <param name="endBefore">UTC date after which no jobs fire.</param>
    /// <param name="metadata">Optional key-value metadata attached to every job occurrence.</param>
    public RecurringScheduleDefinitionCollection Add(
        Type handlerType,
        IRecurrenceCompiledExpr compiledExpr,
        string? defId = null,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        IWritableMetadata? metadata = null)
    {
        if (!typeof(IJobMasterHandler).IsAssignableFrom(handlerType))
        {
            throw new ArgumentException($"{handlerType} must implement IJobMasterHandler.", nameof(handlerType));
        }

        if (!string.IsNullOrEmpty(defId) && !JobMasterStringUtils.IsValidForId(defId!))
        {
            throw new ArgumentException("Invalid DefinitionId", nameof(defId));
        }

        var jobDefinitionId = JobMasterDefinitionIdAttribute.GetJobDefinitionId(handlerType);
        lock (unique)
        {
            var id = GenerateUniqueId(handlerType, defId);
            var definition = new StaticRecurringScheduleDefinition(
                clusterId: string.IsNullOrEmpty(this.profile.ClusterId) ? defaultClusterId : this.profile.ClusterId,
                jobDefinitionId,
                compiledExpr: compiledExpr,
                id: id,
                priority: priority,
                timeout: timeout,
                maxNumberOfRetries: maxNumberOfRetries,
                startAfter: startAfter,
                endBefore: endBefore,
                metadata: metadata,
                workerLane: this.profile.WorkerLane);

            this.Add(definition);
            return this;
        }
    }

    private void Add(StaticRecurringScheduleDefinition definition)
    {
        if (definition == null) throw new ArgumentNullException(nameof(definition));

        var clusterId = string.IsNullOrEmpty(this.profile.ClusterId) ? defaultClusterId : this.profile.ClusterId;

        ValidateDefinition(definition);

        EnsureUnique(clusterId, definition.Id);

        items.Add(definition);
    }

    private static void ValidateId(string? id)
    {
        if (string.IsNullOrWhiteSpace(id))
            throw new ArgumentException("Id is required.", nameof(id));
    }

    private void EnsureUnique(string clusterId, string id)
    {
        var key = (clusterId, id);
        if (!unique.Add(key))
            throw new InvalidOperationException($"Duplicate static schedule Id '{id}' for cluster '{clusterId}'.");
    }

    private static string SanitizeIdPart(string s)
    {
        if (string.IsNullOrEmpty(s)) return "part";
        var sb = new StringBuilder(s.Length);
        foreach (var ch in s)
        {
            // allow letters, digits, '_', '-', '.'
            if (char.IsLetterOrDigit(ch) || ch == '_' || ch == '-' || ch == '.')
                sb.Append(ch);
            else
                sb.Append('_');
        }

        return sb.ToString();
    }

    private string GenerateUniqueId(Type typeHandler, string? defId)
    {
        var profileId = SanitizeIdPart(profile.ProfileId);
        var handler = typeHandler;

        // Prefer attribute TypeId if present; else use short type name
        if (string.IsNullOrWhiteSpace(defId))
        {
            defId = handler.GetCustomAttribute<JobMasterDefinitionIdAttribute>()?.JobDefinitionId ?? handler.Name;
        }

        var defSubId = defId!;
        defSubId = SanitizeIdPart(defSubId);

        // Extremely unlikely, but guard in case of collision within this collection
        var clusterId = string.IsNullOrEmpty(this.profile.ClusterId) ? defaultClusterId : this.profile.ClusterId;
        var candidate = $"{clusterId}:{profileId}:{defSubId}";

        if (!unique.Contains((clusterId, candidate)))
            return candidate;

        // Collision fallback: append a tiny counter until unique
        int counter = 1;
        string withCounter;
        do
        {
            withCounter = $"{candidate}-{counter++}";
        } while (unique.Contains((clusterId, withCounter)));

        return withCounter;
    }

    private static void ValidateDefinition(StaticRecurringScheduleDefinition cfg)
    {
        if (cfg.MaxNumberOfRetries > JobMasterConstants.MaxAllowedRetries)
        {
            throw new ArgumentException(
                $"MaxNumberOfRetries must be less than or equal to {JobMasterConstants.MaxAllowedRetries}.");
        }

        if (cfg.CompiledExpr == null) throw new ArgumentException("CompiledExpr is required.");

        ValidateCompiled(cfg.CompiledExpr);
        ValidateId(cfg.Id);
    }

    private static void ValidateCompiled(IRecurrenceCompiledExpr expr)
    {
        if (string.IsNullOrWhiteSpace(expr.ExpressionTypeId))
            throw new ArgumentException("CompiledExpr.ExpressionTypeId is required.");
        if (string.IsNullOrWhiteSpace(expr.Expression))
            throw new ArgumentException("CompiledExpr.Expression is required.");
    }
}