using JobMaster.Abstractions.RecurrenceExpressions;

namespace JobMaster.Abstractions.StaticRecurringSchedules;

/// <summary>
/// Base class for attributes that declare a static recurring schedule directly on a job handler class,
/// as a lighter alternative to <see cref="IStaticRecurringSchedulesProfile"/> for the common case of one
/// handler with one (or a few) fixed schedules. Discovered and registered automatically at cluster
/// startup, alongside profiles, feeding the same underlying static-schedule machinery (upsert,
/// reconciliation, keep-alive).
/// </summary>
/// <example>
/// <code>
/// [NaturalCronSchedule("every 6 minutes")]
/// public class MyHandler : IJobMasterHandler
/// {
///     public Task HandleAsync(JobContext job) => ...;
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
public abstract class RecurringScheduleAttribute : Attribute
{
    protected RecurringScheduleAttribute(string expression)
    {
        Expression = expression;
    }

    /// <summary>
    /// Must match an <see cref="IRecurrenceExprCompiler.ExpressionTypeId"/> registered with
    /// <see cref="RecurrenceCompilerFactory"/> — the same extension point a custom compiler already
    /// registers through, so a third-party compiler can supply its own <see cref="RecurringScheduleAttribute"/>
    /// subclass the same way the built-in ones do.
    /// </summary>
    public abstract string ExpressionTypeId { get; }

    /// <summary>The raw recurrence expression string, interpreted by the compiler for <see cref="ExpressionTypeId"/>.</summary>
    public string Expression { get; }
}
