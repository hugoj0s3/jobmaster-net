namespace JobMaster.Sdk.Utils.Extensions;

internal static class DateTimeExtensions
{
    internal static DateTime AsUtc(this DateTime dt) => DateTime.SpecifyKind(dt, DateTimeKind.Utc);

    internal static DateTime? AsUtc(this DateTime? dt) => dt.HasValue ? DateTime.SpecifyKind(dt.Value, DateTimeKind.Utc) : (DateTime?)null;
}
