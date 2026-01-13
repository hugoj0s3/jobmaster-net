using System;
using System.Collections.Generic;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using JobMaster.Contracts.Models;
using JobMaster.Contracts.Models.Attributes;
using JobMaster.Contracts.RecurrenceExpressions;
using JobMaster.Contracts.Utils;

namespace JobMaster.Contracts.StaticRecurringSchedules;

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

    public RecurringScheduleDefinitionCollection(StaticRecurringSchedulesProfileInfo profile, string defaultClusterId)
    {
        if (string.IsNullOrWhiteSpace(defaultClusterId))
            throw new ArgumentException("Default clusterId is required", nameof(defaultClusterId));
        this.profile = profile;
        this.defaultClusterId = defaultClusterId;
    }
    
    public IReadOnlyList<StaticRecurringScheduleDefinition> ToReadOnly() => items;
    
    
    public RecurringScheduleDefinitionCollection AddExpr<TH>(
        string expressionType,
        string expression,
        string? defId = null,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        IWritableMetadata? metadata = null)
        where TH : class, IJobHandler
    {
        var compiled = RecurrenceExprCompiler.Compile(expressionType, expression);
        return AddCompiledExpr<TH>(compiled, defId, priority, timeout, startAfter, endBefore, metadata);
    }
    
    public RecurringScheduleDefinitionCollection AddCompiledExpr<TH>(
        IRecurrenceCompiledExpr compiledExpr,
        string? defId = null,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        IWritableMetadata? metadata = null)
        where TH : class, IJobHandler
    {
        if (!string.IsNullOrEmpty(defId) && !JobMasterStringUtils.IsValidForId(defId!))
        {
            throw new ArgumentException("Invalid DefinitionId", nameof(defId));
        }
        
        var jobDefinitionId = typeof(TH).GetCustomAttribute<JobMasterDefinitionIdAttribute>()?.JobDefinitionId ?? typeof(TH).FullName!;
        var id = GenerateUniqueId(typeof(TH), defId);
        var definition = new StaticRecurringScheduleDefinition(
            clusterId: string.IsNullOrEmpty(this.profile.ClusterId) ? defaultClusterId : this.profile.ClusterId,
            jobDefinitionId,
            compiledExpr: compiledExpr,
            id: id,
            priority: priority,
            timeout: timeout,
            startAfter: startAfter,
            endBefore: endBefore,
            metadata: metadata,
            workerLane: this.profile.WorkerLane);

        this.Add(definition);
        return this;
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