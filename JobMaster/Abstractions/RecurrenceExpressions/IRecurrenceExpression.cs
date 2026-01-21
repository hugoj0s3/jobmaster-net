namespace JobMaster.Abstractions.RecurrenceExpressions;

public interface IRecurrenceCompiledExpr
{
    public string Expression { get; }
    public string ExpressionTypeId { get; }

    /// <summary>
    ///     Returns the next occurrence of the recurrence expression.
    ///     Returns null mean there is no next occurrence and the recurrence expression has ended.
    /// </summary>
    /// <param name="dateTime">Datetime in IANA timezone passed in</param>
    /// <param name="ianaTimeZoneId">Timezone of the datetime passed in</param>
    /// <returns></returns>
    DateTime? GetNextOccurrence(DateTime dateTime, string ianaTimeZoneId);
    
    bool HasEnded(DateTime dateTime, string ianaTimeZoneId);
}