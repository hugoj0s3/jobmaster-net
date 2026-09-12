namespace JobMaster.Abstractions.RecurrenceExpressions;

/// <summary>
/// A compiled recurrence expression that never fires.
/// Serves two roles:
/// <list type="bullet">
///   <item><description><b>Null-object default</b> — assigned as the initial value of <see cref="IRecurrenceCompiledExpr"/> properties so they are never <c>null</c> before a real expression is loaded.</description></item>
///   <item><description><b>Termination sentinel</b> — stored on a recurring schedule to permanently stop it from generating further jobs without deleting the schedule record.</description></item>
/// </list>
/// </summary>
public class NeverRecursCompiledExpr : IRecurrenceCompiledExpr
{
    /// <summary>Always returns an empty string — this sentinel has no expression.</summary>
    public string Expression => string.Empty;

    /// <summary>Returns the never-recurs type identifier.</summary>
    public string ExpressionTypeId => NeverRecursExprCompiler.TypeId;

    /// <summary>Always returns <c>null</c> — this expression has no next occurrence.</summary>
    public DateTime? GetNextOccurrence(DateTime dateTime, string ianaTimeZoneId)
    {
        return null;
    }

    /// <summary>Always returns <c>true</c> — the schedule has permanently ended.</summary>
    public bool HasEnded(DateTime dateTime, string ianaTimeZoneId)
    {
        return true;
    }
}
